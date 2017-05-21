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

	toWrite := make([]byte, 8+len(data))

	binary.BigEndian.PutUint32(toWrite[0:4], uint32(len(data)))
	copy(toWrite[4:len(data)+4], data)
	binary.BigEndian.PutUint32(toWrite[len(data)+4:], uint32(len(data)))

	s.data = append(s.data, toWrite...)

	s.last += uint64(len(toWrite))

	return prev, nil
}

func (s *MemoryStore) Close() error {
	return nil
}
