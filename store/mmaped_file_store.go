package store

import (
	"encoding/binary"
	"os"
	"syscall"
)

// FileStore represents a mmapped file
type FileStore struct {
	SliceStore
	file *os.File
}

const maxSegmentSize = 1024*1024*1024*2 - 1

// NewFileStore creates a new instance of memory mapped store.
// If the file can't be open, it returns error.
func NewFileStore(file string) (*FileStore, error) {
	_, err := os.Stat(file)
	var f *os.File
	if os.IsNotExist(err) {
		f, err = os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0700)
		if err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}

		f, err = os.OpenFile(file, os.O_APPEND|os.O_RDWR, 0700)
		if err != nil {
			return nil, err
		}

	}

	length, err := f.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, maxSegmentSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &FileStore{
		file: f,
		SliceStore: SliceStore{
			data: data,
			last: uint64(length),
		},
	}, nil
}

func (s *FileStore) Append(data []byte) (uint64, error) {

	prev := s.last
	toWrite := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(toWrite[0:4], uint32(len(data)))
	copy(toWrite[4:len(data)+4], data)
	binary.BigEndian.PutUint32(toWrite[len(data)+4:], uint32(len(data)))
	_, err := s.file.Write(toWrite)
	if err != nil {
		return 0, err
	}

	s.last += uint64(len(toWrite))

	return prev, nil
}

// Close unmaps the mmaped data and closes the file.
func (s *FileStore) Close() error {
	err := syscall.Munmap(s.data)
	if err != nil {
		return err
	}
	return s.file.Close()
}

func (s *FileStore) BulkAppend(chunks [][]byte) error {

	totalLength := 0
	for _, c := range chunks {
		totalLength += 8 + len(c)
	}

	toWrite := make([]byte, totalLength)

	copied := 0
	for _, c := range chunks {
		binary.BigEndian.PutUint32(toWrite[copied:], uint32(len(c)))
		copied += 4
		copy(toWrite[copied:], c)
		copied += len(c)
		binary.BigEndian.PutUint32(toWrite[copied:], uint32(len(c)))
		copied += 4
	}

	_, err := s.file.Write(toWrite)
	if err != nil {
		return err
	}

	s.last += uint64(len(toWrite))

	return nil

}
