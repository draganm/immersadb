package store

import (
	"encoding/binary"
)

// SliceStore is a parent for any kind of store using in memory slices
type SliceStore struct {
	data []byte
	last uint64
}

func (s *SliceStore) NextChunkAddress() uint64 {
	return s.last
}

// Chunk returns chunk with the given address.
// If the chunk does not exists, it returns nil.
func (s *SliceStore) Chunk(addr uint64) []byte {
	if addr >= s.last {
		return nil
	}

	len := int(binary.BigEndian.Uint16(s.data[addr:])) & 0xffff
	return s.data[int(addr)+2 : int(addr)+len+2]
}

// BytesInStore returns the number of bytes that store has.
func (s *SliceStore) BytesInStore() uint64 {
	return s.last
}
