package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
)

const extendStep = 1 * 1024 * 1024

type SegmentFile struct {
	f                   *os.File
	MMap                mmap.MMap
	maxSize             uint64
	nextFreeByte        int64
	lastSegmentPosition int64
	limit               int64
	useCount            int
	mu                  *sync.Mutex
	useCond             *sync.Cond
	closed              bool
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

	mu := &sync.Mutex{}
	useCond := sync.NewCond(mu)

	return &SegmentFile{
		f:                   f,
		MMap:                mm,
		maxSize:             maxSize,
		nextFreeByte:        offset,
		lastSegmentPosition: lastSegmentPosition,
		limit:               limit,
		mu:                  mu,
		useCond:             useCond,
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

func (s *SegmentFile) ensureNotClosed() {
	if s.closed {
		panic("segment file is closed!")
	}
}

func (s *SegmentFile) StartUse() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureNotClosed()
	s.useCount++
}

func (s *SegmentFile) FinishUse() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureNotClosed()
	if s.useCount <= 0 {
		panic("finishUse called more often than StartUse")
	}
	s.useCount--
	if s.useCount == 0 {
		s.useCond.Broadcast()
	}
}

func (s *SegmentFile) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureNotClosed()

	for s.useCount != 0 {
		s.useCond.Wait()
	}

	err := s.MMap.Unmap()
	if err != nil {
		return errors.Wrapf(err, "while unmmaping %q", s.f.Name())
	}

	s.closed = true

	return s.f.Close()
}

func (s *SegmentFile) Flush() error {
	s.ensureNotClosed()
	return s.MMap.Flush()
}

func (s *SegmentFile) Allocate(size int) (uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureNotClosed()

	if uint64(size)+uint64(s.nextFreeByte) > s.maxSize {
		return 0, nil, errors.Errorf("Cant extend segment %p to %d bytes", s, uint64(size)+uint64(s.nextFreeByte))
	}

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

	s.mu.Lock()
	defer s.mu.Unlock()

	return os.Remove(s.f.Name())
}

func (s *SegmentFile) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureNotClosed()

	return s.nextFreeByte == 0
}

func (s *SegmentFile) UsedBytes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureNotClosed()

	return uint64(s.nextFreeByte)
}

func (s *SegmentFile) CreateEmptySibling() (*SegmentFile, error) {
	s.mu.Lock()

	s.ensureNotClosed()

	fullPath := s.f.Name()
	dir := filepath.Dir(fullPath)
	base := filepath.Base(fullPath)
	maxSize := s.maxSize

	s.mu.Unlock()

	parts := strings.SplitN(base, "-", 2)
	if len(parts) != 2 {
		return nil, errors.Errorf("could not determine prefix of %q", base)
	}

	prefix := parts[0]

	id, err := ksuid.Parse(parts[1])
	if err != nil {
		return nil, errors.Wrapf(err, "while parsing ksuid %q", parts[1])
	}

	return ensureNextLayer(prefix, dir, maxSize, id)
}

func (s *SegmentFile) CanAppend(bytes uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureNotClosed()
	return s.remainingCapacity() >= bytes
}

func (s *SegmentFile) RemainingCapacity() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureNotClosed()
	return s.remainingCapacity()
}

func (s *SegmentFile) remainingCapacity() uint64 {
	return s.maxSize - uint64(s.nextFreeByte)
}
