package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func singleLeft(s store.Store, k store.Address) (store.Address, error) {

	nr := newNodeReader(s, k)
	rcnr := newNodeReader(s, nr.rightChild())

	nlc, err := s.Append(0, func(f store.Segment) error {
		nm := newNodeModifier(f)
		nm.setKey(nr.key())
		nm.setValue(nr.value())
		nm.setLeftChild(nr.leftChild())
		nm.setLeftCount(nr.leftCount())

		nm.setRightChild(rcnr.leftChild())
		nm.setRightCount(rcnr.leftCount())

		return firstError(nr.err, rcnr.err, nm.err)
	})

	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating a'")
	}

	nlccount, err := Count(s, nlc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of a'")
	}

	return s.Append(0,func(f store.Segment) error {
		nm := newNodeModifier(f)

		nm.setValue(rcnr.value())
		nm.setKey(rcnr.key())

		nm.setRightChild(rcnr.rightChild())
		nm.setRightCount(rcnr.rightCount())

		nm.setLeftChild(nlc)
		nm.setLeftCount(nlccount)

		return firstError(nr.err, rcnr.err, nm.err)
	})
}

func doubleLeft(s store.Store, k store.Address) (store.Address, error) {
	nr := newNodeReader(s, k)
	rcnr := newNodeReader(s, nr.rightChild())
	rlcnr := newNodeReader(s, rcnr.leftChild())

	nlc, err := s.Append(0, func(f store.Segment) error {
		nm := newNodeModifier(f)

		nm.setKey(nr.key())
		nm.setValue(nr.value())

		nm.setLeftChild(nr.leftChild())
		nm.setLeftCount(nr.leftCount())

		nm.setRightChild(rlcnr.leftChild())
		nm.setRightCount(rlcnr.leftCount())

		return firstError(nr.err, rcnr.err, rlcnr.err, nm.err)
	})

	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating a'")
	}

	nrc, err := s.Append(0, func(f store.Segment) error {
		nm := newNodeModifier(f)

		nm.setKey(rcnr.key())
		nm.setValue(rcnr.value())

		nm.setLeftChild(rlcnr.rightChild())
		nm.setLeftCount(rlcnr.rightCount())

		nm.setRightChild(rcnr.rightChild())
		nm.setRightCount(rcnr.rightCount())

		return firstError(nr.err, rcnr.err, rlcnr.err, nm.err)
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

		nm.setValue(rlcnr.value())
		nm.setKey(rlcnr.key())

		nm.setLeftCount(nlccount)
		nm.setLeftChild(nlc)

		nm.setRightCount(nrccount)
		nm.setRightChild(nrc)

		return firstError(rlcnr.err, nm.err)
	})
}
