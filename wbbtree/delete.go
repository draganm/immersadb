package wbbtree

import (
	"bytes"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func Delete(s store.Store, root store.Address, key []byte) (store.Address, error) {
	nr, err := delete(s, root, key)
	if err != nil {
		return store.NilAddress, err
	}
	return balance(s, nr)
}

func delete(s store.Store, root store.Address, key []byte) (store.Address, error) {
	if root == store.NilAddress {
		return store.NilAddress, ErrNotFound
	}

	nr, err := newNodeReader(s, root)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	if nr.isEmpty() {
		return store.NilAddress, ErrNotFound
	}

	cmp := bytes.Compare(key, nr.key())

	if cmp == 0 {
		if nr.leftChild() == store.NilAddress && nr.rightChild() == store.NilAddress {
			return store.NilAddress, nil
		}
	}

	switch cmp {
	case 0:
		if nr.leftChild() == store.NilAddress {
			return nr.rightChild(), nil
		}

		if nr.rightChild() == store.NilAddress {
			return nr.leftChild(), nil
		}

		succ, err := findSuccessor(s, nr.rightChild())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while finding successor")
		}

		succRe, err := newNodeReader(s, succ)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node reader")
		}

		newRight, err := Delete(s, nr.rightChild(), succRe.key())

		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while deleting successor")
		}

		nc, err := Count(s, newRight)
		if err != nil {
			return store.NilAddress, err
		}

		nm, err := newNodeModifier(s, succRe.key())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node modifier")
		}
		nm.setLeftChild(nr.leftChild())
		nm.setLeftCount(nr.leftCount())
		nm.setRightChild(newRight)
		nm.setRightCount(nc)
		nm.setValue(succRe.value())

		return nm.Address, nil

	case -1:
		newLeft, err := Delete(s, nr.leftChild(), key)
		if err != nil {
			return store.NilAddress, err
		}

		nm, err := newNodeModifier(s, nr.key())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node modifier")
		}

		nm.setRightChild(nr.rightChild())
		nm.setRightCount(nr.rightCount())
		nm.setValue(nr.value())

		nm.setLeftChild(newLeft)
		nc, err := Count(s, newLeft)
		if err != nil {
			return store.NilAddress, err
		}

		nm.setLeftCount(nc)

		return nm.Address, nil

	case 1:
		newRight, err := Delete(s, nr.rightChild(), key)
		if err != nil {
			return store.NilAddress, err
		}

		nm, err := newNodeModifier(s, nr.key())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node modifier")
		}

		nm.setLeftChild(nr.leftChild())
		nm.setLeftCount(nr.leftCount())
		nm.setValue(nr.value())

		nm.setRightChild(newRight)

		nc, err := Count(s, newRight)
		if err != nil {
			return store.NilAddress, err
		}

		nm.setRightCount(nc)

		return nm.Address, nil

	default:
		return store.NilAddress, errors.New("should never be reached")
	}

}

func findSuccessor(s store.Store, k store.Address) (store.Address, error) {
	nr, err := newNodeReader(s, k)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	lc := nr.leftChild()
	if lc == store.NilAddress {
		return k, nil
	}

	return findSuccessor(s, lc)
}
