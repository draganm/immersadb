package store

import "encoding/binary"

type MemoryStore struct {
	SliceStore
}

func NewMemoryStore(data []byte) *MemoryStore {
	return &MemoryStore{
		SliceStore: SliceStore{
			data: data,
			last: uint64(len(data)),
		},
	}
}

func (s *MemoryStore) Data() []byte {
	return s.data
}

func (s *MemoryStore) Append(data []byte) (uint64, error) {

	prev := s.last

	toWrite := make([]byte, 2+len(data))

	binary.BigEndian.PutUint16(toWrite[0:2], uint16(len(data)))
	copy(toWrite[2:len(data)+2], data)

	s.data = append(s.data, toWrite...)

	s.last += uint64(len(toWrite))

	return prev, nil
}

func (s *MemoryStore) Close() error {
	return nil
}
