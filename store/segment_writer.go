package store

import "github.com/pkg/errors"

import "encoding/binary"

type SegmentWriter struct {
	st Store
	SegmentReader
	Data []byte
	Address
}

func NewSegmentWriter(layer int, st Store, segmentType SegmentType, numberOfChildren int, dataSize int) (SegmentWriter, error) {
	pos, d, err := st[layer].Allocate(4 + 1 + 4*8 + 1 + 8*numberOfChildren + dataSize)
	if err != nil {
		return SegmentWriter{}, errors.Wrap(err, "while creating segment writer")
	}

	binary.BigEndian.PutUint32(d, uint32(len(d)))
	d[4] = byte(segmentType)

	binary.BigEndian.PutUint64(d[4+1:], uint64(len(d)))

	d[4+1+4*8] = byte(numberOfChildren)

	for i := 0; i < numberOfChildren; i++ {
		binary.BigEndian.PutUint64(d[4+1+4*8+1+i*8:], uint64(NilAddress))
	}

	return SegmentWriter{
		st:            st,
		SegmentReader: NewSegmentReader(d),
		Data:          d[4+1+4*8+1+8*numberOfChildren:],
		Address:       NewAddress(0, pos),
	}, nil
}

func (s SegmentWriter) SetLayerTotalSize(i int, newSize uint64) {
	if i < 0 {
		panic("negative layer index")
	}

	if i > 3 {
		panic("not exisiting layer")
	}

	binary.BigEndian.PutUint64(s.SegmentReader[4+1+i*8:], newSize)
}

func (s SegmentWriter) SetChild(i int, addr Address) {

	if i >= s.NumberOfChildren() {
		panic("trying to set child that segment does not have")
	}

	oldChildAddress := s.SegmentReader.GetChildAddress(i)

	if oldChildAddress != NilAddress {
		for i := 0; i < 4; i++ {
			r := NewSegmentReader(s.st.GetSegment(oldChildAddress))
			newSize := s.GetLayerTotalSize(i) - r.GetLayerTotalSize(i)
			s.SetLayerTotalSize(i, newSize)
		}
	}

	binary.BigEndian.PutUint64(s.SegmentReader[4+1+4*8+1+i*8:], uint64(addr))

	if addr == NilAddress {
		return
	}

	newChildReader := NewSegmentReader(s.st.GetSegment(addr))

	for i := 0; i < 4; i++ {
		newSize := s.GetLayerTotalSize(i) + newChildReader.GetLayerTotalSize(i)
		s.SetLayerTotalSize(i, newSize)
	}

}
