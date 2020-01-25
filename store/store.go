package store

import (
	"encoding/binary"
	serrors "errors"
)

type Store []*SegmentFile

var ErrNotFound = serrors.New("not found")

func (s Store) GetSegment(a Address) SegmentReader {
	if a == NilAddress {
		panic("getting Nil Segment")
	}

	idx := a.Segment()
	data := s[idx].MMap

	length := binary.BigEndian.Uint32(data[a.Position():])
	if length == 0 {
		panic("getting segment with length 0")
	}

	return []byte(data[a.Position() : int(a.Position())+int(length)])

}

func (s Store) CreateSegment(layer int, segmentType SegmentType, numberOfChildren int, dataSize int) (SegmentWriter, error) {
	return NewSegmentWriter(layer, s, segmentType, numberOfChildren, dataSize)
}
