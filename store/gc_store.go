package store

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type GCStore struct {
	start uint64
	*FileStore
	fileName string
	dir      string
}

var segmentFilePattern = regexp.MustCompile(`^([a-fA-F0-9]{16}).seg$`)

func NewGCStore(dir string) (*GCStore, error) {

	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	fileNames := []string{}

	for _, e := range entries {
		if !e.IsDir() && segmentFilePattern.MatchString(e.Name()) {
			fileNames = append(fileNames, e.Name())
		}
	}

	sort.Strings(fileNames)

	fileName := ""

	if len(fileNames) == 0 {
		fileName = fmt.Sprintf("%016x.seg", 0)
	}

	if len(fileNames) == 1 {
		fileName = fileNames[0]
	}

	if len(fileNames) > 1 {
		return nil, errors.New("More than one segment found!")
	}

	fn := filepath.Base(fileName)

	start, err := strconv.ParseUint(strings.TrimSuffix(fn, ".seg"), 16, 64)
	if err != nil {
		return nil, err
	}

	pth := filepath.Join(dir, fileName)

	s, err := NewFileStore(pth)
	if err != nil {
		return nil, err
	}

	return &GCStore{start, s, fileName, dir}, nil

}

func (s *GCStore) FirstChunkAddress() uint64 {
	return s.start + s.FileStore.FirstChunkAddress()
}

func (s *GCStore) NextChunkAddress() uint64 {
	return s.start + s.FileStore.NextChunkAddress()
}

func (s *GCStore) Append(data []byte) (uint64, error) {
	addr, err := s.FileStore.Append(data)
	if err != nil {
		return addr, err
	}
	return addr + s.start, nil
}

func (s *GCStore) Chunk(addr uint64) []byte {
	return s.FileStore.Chunk(addr - s.start)
}

func (s *GCStore) GC() error {

	dataSize := Size(s)
	totalSize := s.BytesInStore()

	// don't GC before 10 mb
	if dataSize < 10*1024*1024 {
		return nil
	}

	if totalSize > uint64(float64(dataSize)*1.5) {

		newStart := s.NextChunkAddress()
		fileName := fmt.Sprintf("%016x.seg", newStart)
		newFileStore, err := NewFileStore(filepath.Join(s.dir, fileName))
		if err != nil {
			return err
		}

		newStore := &GCStore{newStart, newFileStore, fileName, s.dir}

		err = Copy(s, newStore)
		if err != nil {
			return err
		}

		err = s.FileStore.Close()
		if err != nil {
			return err
		}

		err = os.Remove(filepath.Join(s.dir, s.fileName))
		if err != nil {
			return err
		}

		s.start = newStart
		s.FileStore = newFileStore
		s.fileName = fileName

	}

	return nil
}
