package store

import "github.com/pkg/errors"

import "encoding/binary"

type SegmentWriter struct {
	st   Store
	seg  []byte
	Data []byte
	Address
}

func NewSegmentWriter(st Store, segmentType byte, numberOfChildren int, dataSize int) (SegmentWriter, error) {
	pos, d, err := st[0].Allocate(4 + 1 + 4*8 + 1 + 8*numberOfChildren + dataSize)
	if err != nil {
		return SegmentWriter{}, errors.Wrap(err, "while creating segment writer")
	}

	binary.BigEndian.PutUint32(d, uint32(len(d)))
	d[4] = segmentType

	binary.BigEndian.PutUint64(d[4+1:], uint64(len(d)))

	d[4+1+4*8] = byte(numberOfChildren)

	for i := 0; i < numberOfChildren; i++ {
		binary.BigEndian.PutUint64(d[4+1+4*8+1+i*8:], uint64(NilAddress))
	}

	return SegmentWriter{
		st:      st,
		seg:     d,
		Data:    d[4+1+4*8+1+8*numberOfChildren:],
		Address: NewAddress(0, pos),
	}, nil
}

// func (s SegmentWriter)
