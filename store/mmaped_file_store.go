package store

import (
	"encoding/binary"
	"log"
	"os"
	"syscall"
)

// FileStore represents a mmapped file
type FileStore struct {
	SliceStore
	file *os.File
}

const maxSegmentSize = 10 * 1024 * 1024 * 2

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
		log.Println(err)

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
	toWrite := make([]byte, 2+len(data))
	binary.BigEndian.PutUint16(toWrite[0:2], uint16(len(data)))
	copy(toWrite[2:len(data)+2], data)
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

func (s *FileStore) Delete() error {
	return os.Remove(s.file.Name())
}

func (s *FileStore) BulkAppend(chunks [][]byte) error {

	totalLength := 0
	for _, c := range chunks {
		totalLength += 2 + len(c)
	}

	toWrite := make([]byte, totalLength)

	copied := 0
	for _, c := range chunks {
		binary.BigEndian.PutUint16(toWrite[copied:], uint16(len(c)))
		copied += 2
		copy(toWrite[copied:], c)
		copied += len(c)
	}

	_, err := s.file.Write(toWrite)
	if err != nil {
		return err
	}

	s.last += uint64(len(toWrite))

	return nil

}
