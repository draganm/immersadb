package store

import (
	"encoding/binary"
)

// SliceStore is a parent for any kind of store using in memory slices
type SliceStore struct {
	data []byte
	last uint64
}

// LastChunkAddress returns the address of the last chunk.
// If there are no chunks in the store it returns 0!
func (s *SliceStore) LastChunkAddress() uint64 {
	if s.last == 0 {
		return 0
	}
	len := binary.BigEndian.Uint32(s.data[s.last-4:])

	return s.last - uint64(8+len)
}

// LastChunk returns the content of the last chunk in the store (last commit).
// When there are no chunks in the store, it return nil.
func (s *SliceStore) LastChunk() []byte {
	if s.last == 0 {
		return nil
	}
	len := binary.BigEndian.Uint32(s.data[s.last-4:])

	return s.Chunk(s.last - uint64(8+len))
}

// Chunk returns chunk with the given address.
// If the chunk does not exists, it returns nil.
func (s *SliceStore) Chunk(addr uint64) []byte {
	if addr >= s.last {
		return nil
	}
	len := int(binary.BigEndian.Uint32(s.data[addr:]))
	return s.data[int(addr)+4 : int(addr)+len+4]
}

// BytesInStore returns the number of bytes that store has.
func (s *SliceStore) BytesInStore() uint64 {
	return s.last
}
