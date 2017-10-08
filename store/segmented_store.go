package store

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Segment struct {
	start uint64
	*FileStore
}

type SegmentedStore struct {
	fullSegments   []*Segment
	currentSegment *Segment
	MaxSegmentSize int
	dir            string
}

func (s *Segment) endAddress() uint64 {
	return s.start + s.FileStore.NextChunkAddress()
}

var segmentFilePattern = regexp.MustCompile(`^([a-fA-F0-9]{16}).seg$`)

func NewSegmentedStore(dir string, maxSegmentSize int) (*SegmentedStore, error) {
	segments := []*Segment{}

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

	for _, fn := range fileNames {
		start, err := strconv.ParseUint(strings.TrimSuffix(fn, ".seg"), 16, 64)
		if err != nil {
			return nil, err
		}
		pth := filepath.Join(dir, fn)
		fs, err := NewFileStore(pth)
		if err != nil {

			return nil, err
		}

		segments = append(segments, &Segment{start: start, FileStore: fs})

	}

	if len(segments) == 0 {
		fs, err := NewFileStore(filepath.Join(dir, fmt.Sprintf("%016x.seg", 0)))
		if err != nil {
			return nil, err
		}

		segments = append(segments, &Segment{start: uint64(0), FileStore: fs})

	}

	return &SegmentedStore{
		fullSegments:   segments[:len(segments)-1],
		currentSegment: segments[len(segments)-1],
		MaxSegmentSize: maxSegmentSize,
		dir:            dir,
	}, nil
}

func (ss *SegmentedStore) DropBefore(addr uint64) error {
	for len(ss.fullSegments) > 0 && ss.fullSegments[0].endAddress() <= addr {
		seg := ss.fullSegments[0]
		err := seg.Close()
		if err != nil {
			return err
		}
		err = seg.Delete()
		if err != nil {
			return err
		}
		ss.fullSegments = ss.fullSegments[1:]
	}
	return nil
}

func (ss *SegmentedStore) Append(data []byte) (uint64, error) {
	if int(ss.currentSegment.BytesInStore())+len(data)+4 > ss.MaxSegmentSize {
		start := ss.currentSegment.start + ss.currentSegment.BytesInStore()
		fileName := fmt.Sprintf("%016x.seg", start)
		fs, err := NewFileStore(filepath.Join(ss.dir, fileName))
		if err != nil {
			return 0, err
		}

		ss.fullSegments = append(ss.fullSegments, ss.currentSegment)
		ss.currentSegment = &Segment{start: start, FileStore: fs}
	}

	addr, err := ss.currentSegment.Append(data)
	if err != nil {
		return 0, err
	}

	return addr + ss.currentSegment.start, nil

}

func (ss *SegmentedStore) Chunk(addr uint64) []byte {
	if ss.currentSegment.start <= addr {
		return ss.currentSegment.Chunk(addr - ss.currentSegment.start)
	}

	for i := len(ss.fullSegments) - 1; i >= 0; i-- {
		s := ss.fullSegments[i]
		if s.start <= addr {
			return s.Chunk(addr - s.start)
		}
	}

	// probably never reached
	return nil
}

func (ss *SegmentedStore) NextChunkAddress() uint64 {
	return ss.currentSegment.start + ss.currentSegment.NextChunkAddress()
}

func (ss *SegmentedStore) BytesInStore() uint64 {
	count := uint64(0)
	for _, s := range ss.fullSegments {
		count += s.BytesInStore()
	}
	return count + ss.currentSegment.BytesInStore()
}

func (ss *SegmentedStore) Close() error {
	err := ss.currentSegment.Close()
	if err != nil {
		return err
	}
	for _, s := range ss.fullSegments {
		err = s.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (ss *SegmentedStore) BulkAppend(chunks [][]byte) error {

	for _, c := range chunks {
		_, err := ss.Append(c)
		if err != nil {
			return err
		}
	}
	return nil

	// canAppend := ss.MaxSegmentSize - int(ss.currentSegment.BytesInStore())
	// appendToCurrent := [][]byte{}
	// for _, c := range chunks {
	// 	toAppend := len(c) + 4
	// 	if toAppend <= canAppend {
	// 		appendToCurrent = append(appendToCurrent, c)
	// 	} else {
	// 		if len(appendToCurrent) > 0 {
	// 			log.Println("creating new segment")
	// 			err := ss.currentSegment.BulkAppend(appendToCurrent)
	// 			if err != nil {
	// 				return err
	// 			}
	//
	// 			start := ss.currentSegment.start + ss.currentSegment.NextChunkAddress()
	// 			fileName := fmt.Sprintf("%016x.seg", start)
	// 			fs, err := NewFileStore(filepath.Join(ss.dir, fileName))
	// 			if err != nil {
	// 				return err
	// 			}
	//
	// 			ss.fullSegments = append(ss.fullSegments, ss.currentSegment)
	// 			ss.currentSegment = &Segment{start: start, FileStore: fs}
	//
	// 			appendToCurrent = [][]byte{c}
	// 			canAppend = ss.MaxSegmentSize
	// 		}
	// 	}
	// 	canAppend -= toAppend
	// }
	//
	// if len(appendToCurrent) > 0 {
	// 	err := ss.currentSegment.BulkAppend(appendToCurrent)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// return nil
}
