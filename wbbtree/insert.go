package wbbtree

import (
	"bytes"
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func Insert(s store.Store, root store.Address, key []byte, value store.Address) (store.Address, error) {
	nr, err := insert(s, root, key, value)
	if err != nil {
		return store.NilAddress, err
	}

	return balance(s, nr)
}

func insert(s store.Store, root store.Address, key []byte, value store.Address) (store.Address, error) {
	if root == store.NilAddress {
		nm, err := newNodeModifier(s, key)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node modifier")
		}

		nm.setValue(value)
		return nm.Address, nil
	}

	nr, err := newNodeReader(s, root)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}

	if nr.isEmpty() {
		nm, err := newNodeModifier(s, key)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node modifier")
		}

		nm.setValue(value)
		return nm.Address, nil
	}

	cmp := bytes.Compare(key, nr.key())

	switch cmp {
	case 0:
		nm, err := newNodeModifier(s, nr.key())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node modifier")
		}
		nm.setLeftChild(nr.leftChild())
		nm.setRightChild(nr.rightChild())
		nm.setLeftCount(nr.leftCount())
		nm.setRightCount(nr.rightCount())
		nm.setValue(value)

		return nm.Address, nil

	case -1:
		newLeft, err := Insert(s, nr.leftChild(), key, value)
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
		newRight, err := Insert(s, nr.rightChild(), key, value)
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
