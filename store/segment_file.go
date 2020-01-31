package store

import (
	"encoding/binary"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

const extendStep = 1 * 1024 * 1024

type SegmentFile struct {
	f                   *os.File
	MMap                mmap.MMap
	maxSize             uint64
	nextFreeByte        int64
	lastSegmentPosition int64
	limit               int64
}

func OpenOrCreateSegmentFile(fileName string, maxSize uint64) (*SegmentFile, error) {

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	fs, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "while getting stats of file %q", fileName)
	}

	mm, err := mmap.MapRegion(f, int(maxSize), mmap.RDWR, 0, 0)
	if err != nil {
		f.Close()
		return nil, errors.Wrapf(err, "while mmaping file %q", fileName)
	}

	offset := int64(0)

	limit := fs.Size()

	var lastSegmentPosition int64

	for offset+4 < limit {
		skip := int64(binary.BigEndian.Uint32(mm[offset:]))
		if skip == int64(0) {
			break
		}
		lastSegmentPosition = offset
		offset += skip
	}

	return &SegmentFile{
		f:                   f,
		MMap:                mm,
		maxSize:             maxSize,
		nextFreeByte:        offset,
		lastSegmentPosition: lastSegmentPosition,
		limit:               limit,
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

func (s *SegmentFile) Flush() error {
	return s.MMap.Flush()
}

func (s *SegmentFile) Allocate(size int) (uint64, []byte, error) {
	err := s.ensureSize(int(s.nextFreeByte) + size)
	if err != nil {
		return 0, nil, errors.Wrap(err, "while ensuring size")
	}
	start := s.nextFreeByte
	s.nextFreeByte += int64(size)
	s.lastSegmentPosition = start
	return uint64(start), s.MMap[int(start) : int(start)+size], nil
}

func (s *SegmentFile) CloseAndDelete() error {
	err := s.Close()
	if err != nil {
		return errors.Wrap(err, "while closing layer")
	}

	return os.Remove(s.f.Name())
}

func (s *SegmentFile) IsEmpty() bool {
	return s.nextFreeByte == 0
}