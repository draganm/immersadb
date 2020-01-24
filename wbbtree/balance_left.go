package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func singleLeft(s store.Store, k store.Address) (store.Address, error) {

	nr, err := newNodeReader(s, k)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	rcnr, err := newNodeReader(s, nr.rightChild())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	nm, err := newNodeModifier(s, nr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}
	nm.setValue(nr.value())
	nm.setLeftChild(nr.leftChild())
	nm.setLeftCount(nr.leftCount())

	nm.setRightChild(rcnr.leftChild())
	nm.setRightCount(rcnr.leftCount())

	nlc := nm.Address
	nlccount, err := Count(s, nlc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of a'")
	}

	nm, err = newNodeModifier(s, rcnr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(rcnr.value())

	nm.setRightChild(rcnr.rightChild())
	nm.setRightCount(rcnr.rightCount())

	nm.setLeftChild(nlc)
	nm.setLeftCount(nlccount)

	return nm.Address, nil
}

func doubleLeft(s store.Store, k store.Address) (store.Address, error) {
	nr, err := newNodeReader(s, k)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	rcnr, err := newNodeReader(s, nr.rightChild())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	rlcnr, err := newNodeReader(s, rcnr.leftChild())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	nm, err := newNodeModifier(s, nr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(nr.value())

	nm.setLeftChild(nr.leftChild())
	nm.setLeftCount(nr.leftCount())

	nm.setRightChild(rlcnr.leftChild())
	nm.setRightCount(rlcnr.leftCount())

	nlc := nm.Address

	nm, err = newNodeModifier(s, rcnr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(rcnr.value())

	nm.setLeftChild(rlcnr.rightChild())
	nm.setLeftCount(rlcnr.rightCount())

	nm.setRightChild(rcnr.rightChild())
	nm.setRightCount(rcnr.rightCount())

	nrc := nm.Address

	nlccount, err := Count(s, nlc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of a'")
	}

	nrccount, err := Count(s, nrc)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting count of c'")
	}

	nm, err = newNodeModifier(s, rlcnr.key())
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}

	nm.setValue(rlcnr.value())

	nm.setLeftCount(nlccount)
	nm.setLeftChild(nlc)

	nm.setRightCount(nrccount)
	nm.setRightChild(nrc)

	return nm.Address, nil

}
