package store

import (
	"encoding/binary"
	"errors"
)

// layout
// total length: 4 bytes
// type: byte
// layer_sizes: 4 * 8 bytes
// number_of_children: 1 byte
// number_of_children * 8 bytes

type SegmentReader []byte

func NewSegmentReader(data []byte) SegmentReader {
	if len(data) < 4 {
		panic(errors.New("segment data is too short"))
	}

	totalLength := int(binary.BigEndian.Uint32(data))

	if len(data) < totalLength {
		panic(errors.New("segment data is too short"))
	}

	if totalLength < 4+1+4*8+1 {
		panic(errors.New("total length is too short"))
	}

	numberOfChildren := data[4+1+4*8]

	headerLength := numberOfChildren*8 + 4 + 1 + 4*8 + 1

	if int(headerLength) > totalLength {
		panic(errors.New("total length is too short"))
	}

	return data[:totalLength]

}

func (s SegmentReader) NumberOfChildren() int {
	return int(s[4+1+4*8])
}

func (s SegmentReader) GetChildAddress(i int) Address {
	if i < 0 {
		panic("negative child index")
	}

	if i >= s.NumberOfChildren() {
		panic("trying to get address of not existing child")
	}

	return Address(binary.BigEndian.Uint64(s[4+1+4*8+1+8*i:]))
}

func (s SegmentReader) GetData() []byte {
	nc := s.NumberOfChildren()
	return s[4+1+4*8+1+8*nc:]
}

func (s SegmentReader) GetLayerTotalSize(l int) uint64 {
	if l < 0 {
		panic("layer number can't be negative")
	}
	if l > 3 {
		panic("layer number can't be >3")
	}

	return binary.BigEndian.Uint64(s[4+1+l*8:])
}

func (s SegmentReader) GetTotalTreeSize() uint64 {
	var totalSize uint64
	for i := 0; i < 4; i++ {
		totalSize += s.GetLayerTotalSize(i)
	}
	return totalSize
}

func (s SegmentReader) Type() SegmentType {
	return SegmentType(s[4])
}
