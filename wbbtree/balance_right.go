package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func singleRight(s store.Store, k store.Address) (store.Address, error) {

	nr := newNodeReader(s, k)
	lcnr := newNodeReader(s, nr.leftChild())

	nrc, err := s.Append(0,func(f store.Segment) error {
		nm := newNodeModifier(f)
		nm.setKey(nr.key())
		nm.setValue(nr.value())
		nm.setRightChild(nr.rightChild())
		nm.setRightCount(nr.rightCount())

		nm.setLeftChild(lcnr.rightChild())
		nm.setLeftCount(lcnr.rightCount())

		return firstError(nr.err, lcnr.err, nm.err)
	})

	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating a'")
	}

	nrccount, err := Count(s, nrc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of a'")
	}

	return s.Append(0,func(f store.Segment) error {
		nm := newNodeModifier(f)

		nm.setValue(lcnr.value())
		nm.setKey(lcnr.key())

		nm.setLeftChild(lcnr.leftChild())
		nm.setLeftCount(lcnr.leftCount())

		nm.setRightChild(nrc)
		nm.setRightCount(nrccount)

		return firstError(nr.err, lcnr.err, nm.err)
	})
}

func doubleRight(s store.Store, k store.Address) (store.Address, error) {
	nr := newNodeReader(s, k)
	lcnr := newNodeReader(s, nr.leftChild())
	lrcnr := newNodeReader(s, lcnr.rightChild())

	nrc, err := s.Append(0,func(f store.Segment) error {
		nm := newNodeModifier(f)

		nm.setKey(nr.key())
		nm.setValue(nr.value())

		nm.setRightChild(nr.rightChild())
		nm.setRightCount(nr.rightCount())

		nm.setLeftChild(lrcnr.rightChild())
		nm.setLeftCount(lrcnr.rightCount())

		return firstError(nr.err, lcnr.err, lrcnr.err, nm.err)
	})

	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating a'")
	}

	nlc, err := s.Append(0,func(f store.Segment) error {
		nm := newNodeModifier(f)

		nm.setKey(lcnr.key())
		nm.setValue(lcnr.value())

		nm.setRightChild(lrcnr.leftChild())
		nm.setRightCount(lrcnr.leftCount())

		nm.setLeftChild(lcnr.leftChild())
		nm.setLeftCount(lcnr.leftCount())

		return firstError(nr.err, lcnr.err, lrcnr.err, nm.err)
	})

	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating c'")
	}

	nlccount, err := Count(s, nlc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of a'")
	}

	nrccount, err := Count(s, nrc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of c'")
	}

	return s.Append(0,func(f store.Segment) error {
		nm := newNodeModifier(f)

		nm.setValue(lrcnr.value())
		nm.setKey(lrcnr.key())

		nm.setRightCount(nrccount)
		nm.setRightChild(nrc)

		nm.setLeftCount(nlccount)
		nm.setLeftChild(nlc)

		return firstError(lrcnr.err, nm.err)
	})
}
