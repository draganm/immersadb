package store

import (
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

const extendStep = 1 * 1024 * 1024

type SegmentFile struct {
	f            *os.File
	MMap         mmap.MMap
	maxSize      uint64
	nextFreeByte int64
	limit        int64
}

func OpenOrCreateSegmentFile(fileName string, maxSize uint64) (*SegmentFile, error) {

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	fs, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "while getting stats of %q", fileName)
	}

	mm, err := mmap.MapRegion(f, int(maxSize), mmap.RDWR, 0, 0)
	if err != nil {
		f.Close()
		return nil, errors.Wrapf(err, "while mmaping file %q", fileName)
	}

	return &SegmentFile{
		f:            f,
		MMap:         mm,
		maxSize:      maxSize,
		nextFreeByte: fs.Size(),
		limit:        fs.Size(),
	}, nil
}

func (s *SegmentFile) ensureSize(bytes int) error {
	for int(s.limit-s.nextFreeByte) < bytes {
		// TODO: figure out how to do only one truncate
		err := s.f.Truncate(int64(s.nextFreeByte + extendStep))
		if err != nil {
			return errors.Wrapf(err, "while extending file %q to %d bytes", s.f.Name(), extendStep)
		}
		s.limit += extendStep
	}

	return nil
}

func (s *SegmentFile) Close() error {
	err := s.MMap.Unmap()
	if err != nil {
		return errors.Wrapf(err, "while unmmaping %q", s.f.Name())
	}

	return s.f.Close()
}

func (s *SegmentFile) Write(data []byte) (uint64, error) {
	err := s.ensureSize(int(s.nextFreeByte) + len(data))
	if err != nil {
		return 0, errors.Wrap(err, "while ensuring size")
	}

	copy(s.MMap[s.nextFreeByte:s.limit], data)

	addr := s.nextFreeByte

	s.nextFreeByte += int64(len(data))

	return uint64(addr), nil
}

func (s *SegmentFile) Flush() error {
	return s.MMap.Flush()
}
