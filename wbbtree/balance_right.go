package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func singleRight(s store.Store, k store.Address) (store.Address, error) {

	nr, err := newNodeReader(s, k)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	lcnr, err := newNodeReader(s, nr.leftChild())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	nm, err := newNodeModifier(s, nr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(nr.value())
	nm.setRightChild(nr.rightChild())
	nm.setRightCount(nr.rightCount())

	nm.setLeftChild(lcnr.rightChild())
	nm.setLeftCount(lcnr.rightCount())

	nrc := nm.Address

	nrccount, err := Count(s, nrc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of a'")
	}

	nm, err = newNodeModifier(s, lcnr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}
	nm.setValue(lcnr.value())

	nm.setLeftChild(lcnr.leftChild())
	nm.setLeftCount(lcnr.leftCount())

	nm.setRightChild(nrc)
	nm.setRightCount(nrccount)

	return nm.Address, nil
}

func doubleRight(s store.Store, k store.Address) (store.Address, error) {
	nr, err := newNodeReader(s, k)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	lcnr, err := newNodeReader(s, nr.leftChild())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	lrcnr, err := newNodeReader(s, lcnr.rightChild())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	nm, err := newNodeModifier(s, nr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(nr.value())

	nm.setRightChild(nr.rightChild())
	nm.setRightCount(nr.rightCount())

	nm.setLeftChild(lrcnr.rightChild())
	nm.setLeftCount(lrcnr.rightCount())

	nrc := nm.Address

	nm, err = newNodeModifier(s, lcnr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(lcnr.value())

	nm.setRightChild(lrcnr.leftChild())
	nm.setRightCount(lrcnr.leftCount())

	nm.setLeftChild(lcnr.leftChild())
	nm.setLeftCount(lcnr.leftCount())

	nlc := nm.Address

	nlccount, err := Count(s, nlc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of a'")
	}

	nrccount, err := Count(s, nrc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of c'")
	}

	nm, err = newNodeModifier(s, lrcnr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(lrcnr.value())

	nm.setRightCount(nrccount)
	nm.setRightChild(nrc)

	nm.setLeftCount(nlccount)
	nm.setLeftChild(nlc)

	return nm.Address, nil
}
