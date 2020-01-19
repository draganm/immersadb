package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type nodeReader struct {
	f store.Segment
	e error
}

func newNodeReader(st store.Store, k store.Address) *nodeReader {

	nr := &nodeReader{store.Segment{}, nil}

	f, err := st.Get(k)
	if err != nil {
		nr.setError(errors.Wrapf(err, "while getting segment with key %s", k))
		return nr
	}

	nr.f = f

	if f.Specific().Which() != store.Segment_specific_Which_wbbtreeNode {
		nr.setError(errors.Errorf("Wrong type of segment: %s", f.Specific().Which()))
		return nr
	}

	ch, err := f.Children()
	if err != nil {
		nr.setError(errors.Wrap(err, "while getting wbbtree segment children"))
		return nr
	}

	if ch.Len() != 3 {
		nr.setError(errors.Wrapf(err, "Expected wbbtree segment to have 3 children, but got %d", ch.Len()))
		return nr
	}

	return nr
}

func (n *nodeReader) err() error {
	return n.e
}

func (n *nodeReader) setError(err error) {
	if n.e == nil {
		n.e = err
	}
}

func (n *nodeReader) leftChild() store.Address {
	if n.e != nil {
		return store.NilAddress
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree segment children"))
		return store.NilAddress
	}

	return store.Address(ch.At(0))
}

func (n *nodeReader) rightChild() store.Address {
	if n.e != nil {
		return store.NilAddress
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree segment children"))
		return store.NilAddress
	}

	return store.Address(ch.At(1))
}

func (n *nodeReader) value() store.Address {
	if n.e != nil {
		return store.NilAddress
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree segment children"))
		return store.NilAddress
	}

	return store.Address(ch.At(2))
}

func (n *nodeReader) key() []byte {
	if n.e != nil {
		return nil
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node data"))
		return nil
	}

	k, err := tn.Key()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node key"))
		return nil
	}

	return k
}

func (n *nodeReader) leftCount() uint64 {
	if n.e != nil {
		return 0
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node data"))
		return 0
	}

	return tn.CountLeft()
}

func (n *nodeReader) rightCount() uint64 {
	if n.e != nil {
		return 0
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node data"))
		return 0
	}

	return tn.CountRight()
}
