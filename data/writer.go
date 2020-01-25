package data

import (
	"encoding/binary"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type fragmentAggregator struct {
	maxfragments int
	store        store.Store
	parent       *fragmentAggregator
	fragments    []store.Address
	totalSize    uint64
}

func (f *fragmentAggregator) addFragment(k store.Address, size uint64) error {
	if len(f.fragments) >= f.maxfragments {
		if f.parent == nil {
			f.parent = newFragmentAggregator(f.maxfragments, f.store)
		}

		nf, err := f.toSegment()
		if err != nil {
			return errors.Wrap(err, "while creating aggregated fragment")
		}

		err = f.parent.addFragment(nf, f.totalSize)
		if err != nil {
			return errors.Wrap(err, "while adding new aggregated fragment to parent")
		}
		f.fragments = nil
		f.totalSize = 0
	}
	f.fragments = append(f.fragments, k)
	f.totalSize += size
	return nil
}

func (f *fragmentAggregator) toSegment() (store.Address, error) {

	sw, err := f.store.CreateSegment(0, store.TypeDataNode, len(f.fragments), 8)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating segment")
	}

	for i, k := range f.fragments {
		sw.SetChild(i, k)
	}

	binary.BigEndian.PutUint64(sw.Data, f.totalSize)

	return sw.Address, nil

}

func (f *fragmentAggregator) finish() (store.Address, error) {
	if f.parent == nil {
		if len(f.fragments) == 0 {
			sw, err := f.store.CreateSegment(0, store.TypeDataLeaf, 0, 0)

			if err != nil {
				return store.NilAddress, errors.Wrap(err, "while creating empty data leaf")
			}
			return sw.Address, nil
		}
		if len(f.fragments) == 1 {
			return f.fragments[0], nil
		}
		return f.toSegment()
	}

	if len(f.fragments) != 0 {
		k, err := f.toSegment()
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating new fragment on finish")
		}
		err = f.parent.addFragment(k, f.totalSize)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while adding new fragment to parent on finish")
		}
	}

	return f.parent.finish()
}

func newFragmentAggregator(maxfragments int, store store.Store) *fragmentAggregator {
	return &fragmentAggregator{
		maxfragments: maxfragments,
		store:        store,
	}
}

type DataWriter struct {
	fragSize int
	fanout   int

	parentAggregator *fragmentAggregator

	children []store.Address

	buffer []byte

	store store.Store
}

func NewDataWriter(store store.Store, fragSize, fanout int) *DataWriter {
	return &DataWriter{
		fragSize:         fragSize,
		fanout:           fanout,
		store:            store,
		parentAggregator: newFragmentAggregator(fanout, store),
	}
}

func (dw *DataWriter) Write(d []byte) (int, error) {
	written := 0
	for len(d) > 0 {
		lim := len(d)
		if lim > dw.fragSize-len(dw.buffer) {
			lim = dw.fragSize - len(dw.buffer)
		}
		dw.buffer = append(dw.buffer, d[:lim]...)
		written += lim
		d = d[lim:]

		if len(dw.buffer) > dw.fragSize {
			return -1, errors.New("invariant violation: buffer is bigger than fragSize")
		}

		if len(dw.buffer) == dw.fragSize {
			sw, err := dw.store.CreateSegment(0, store.TypeDataLeaf, 0, len(dw.buffer))
			if err != nil {
				return -1, errors.Wrap(err, "while storing data leaf")
			}
			copy(sw.Data, dw.buffer)
			err = dw.parentAggregator.addFragment(sw.Address, uint64(len(dw.buffer)))
			if err != nil {
				return -1, errors.Wrap(err, "while adding fragment to leaf's parent")
			}
			dw.buffer = nil
		}
	}

	return written, nil
}

func (dw *DataWriter) Finish() (store.Address, error) {
	if len(dw.buffer) > 0 {
		sw, err := dw.store.CreateSegment(0, store.TypeDataLeaf, 0, len(dw.buffer))
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while storing data leaf")
		}

		copy(sw.Data, dw.buffer)

		err = dw.parentAggregator.addFragment(sw.Address, uint64(len(dw.buffer)))
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while adding data fragmment to it's aggregator")
		}

	}

	return dw.parentAggregator.finish()

}
