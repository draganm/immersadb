package store

import (
	"encoding/binary"
	serrors "errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
)

type Store []*SegmentFile

var ErrNotFound = serrors.New("not found")

func (s Store) GetSegment(a Address) SegmentReader {
	if a == NilAddress {
		panic("getting Nil Segment")
	}

	idx := a.Segment()
	data := s[idx].MMap

	length := binary.BigEndian.Uint32(data[a.Position():])
	if length == 0 {
		panic("getting segment with length 0")
	}

	return []byte(data[a.Position() : int(a.Position())+int(length)])

}

func (s Store) CreateSegment(layer int, segmentType SegmentType, numberOfChildren int, dataSize int) (SegmentWriter, error) {
	return NewSegmentWriter(layer, s, segmentType, numberOfChildren, dataSize)
}

func filesWithPrefixSorted(prefix string, infos []os.FileInfo) []string {
	prefixed := []string{}
	for _, fi := range infos {
		fn := filepath.Base(fi.Name())
		if fi.Mode().IsRegular() && strings.HasPrefix(fn, prefix) {
			prefixed = append(prefixed, fi.Name())
		}
	}

	sort.Strings(prefixed)

	return prefixed
}

func ensureLayer(prefix, dir string, infos []os.FileInfo, maxSize uint64) (*SegmentFile, error) {
	files := filesWithPrefixSorted(prefix, infos)

	var fileName string

	if len(files) == 0 {
		fileName = filepath.Join(dir, fmt.Sprintf("%s-%s", prefix, ksuid.New().String()))
	} else {
		fileName = files[len(files)-1]
	}

	return OpenOrCreateSegmentFile(fileName, maxSize)
}

type layer struct {
	prefix  string
	maxSize uint64
}

var layers = []layer{
	layer{
		prefix:  "l1",
		maxSize: 10 * 1024 * 1024,
	},
	layer{
		prefix:  "l2",
		maxSize: 1024 * 1024 * 1024,
	},
	layer{
		prefix:  "l3",
		maxSize: 1024 * 1024 * 1024 * 1024,
	},
}

func Open(dir string) (Store, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "while listing dir %q", dir)
	}

	st := make(Store, 4)

	for i, l := range layers {
		sf, err := ensureLayer(l.prefix, dir, files, l.maxSize)
		if err != nil {
			return nil, errors.Wrapf(err, "while ensuring layer %d", i)
		}

		st[i+1] = sf
	}

	return st, nil

}

func (s Store) WithTransaction() (Store, error) {
	st := make(Store, 4)
	copy(st, s)

	dir := filepath.Dir(s[1].f.Name())

	sf, err := ensureLayer("transaction", dir, nil, 10*1024*1024)
	if err != nil {
		return nil, errors.Wrap(err, "while creating transaction layer")
	}

	st[0] = sf

	return st, nil
}

func (s Store) IsEmpty() bool {
	for _, l := range s {
		if l != nil {
			if !l.IsEmpty() {
				return false
			}
		}
	}

	return true
}

func (s Store) Root() Address {
	for i, l := range s {
		if l != nil {
			if !l.IsEmpty() {
				return NewAddress(i, uint64(l.lastSegmentPosition))
			}
		}
	}

	panic("store is empty")
}
