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

	nr := newNodeReader(s, root)

	cmp := bytes.Compare(key, nr.key())

	if cmp == 0 {
		if nr.leftChild() == store.NilAddress && nr.rightChild() == store.NilAddress {
			return store.NilAddress, nr.err()
		}
	}

	switch cmp {
	case 0:
		if nr.leftChild() == store.NilAddress {
			return nr.rightChild(), nr.err()
		}

		if nr.rightChild() == store.NilAddress {
			return nr.leftChild(), nr.err()
		}

		succ, err := findSuccessor(s, nr.rightChild())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while finding successor")
		}

		succRe := newNodeReader(s, succ)

		newRight, err := Delete(s, nr.rightChild(), succRe.key())

		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while deleting successor")
		}

		nc, err := Count(s, newRight)
		if err != nil {
			return store.NilAddress, err
		}

		return s.Append(0,func(f store.Segment) error {
			nm := newNodeModifier(f)
			nm.setLeftChild(nr.leftChild())
			nm.setLeftCount(nr.leftCount())
			nm.setRightChild(newRight)
			nm.setRightCount(nc)
			nm.setKey(succRe.key())
			nm.setValue(succRe.value())

			if nr.err() != nil {
				return nr.err()
			}

			if succRe.err() != nil {
				return succRe.err()
			}
			return nm.err()
		})

	case -1:
		newLeft, err := Delete(s, nr.leftChild(), key)
		if err != nil {
			return store.NilAddress, err
		}

		return s.Append(0,func(f store.Segment) error {
			nm := newNodeModifier(f)
			nm.setRightChild(nr.rightChild())
			nm.setRightCount(nr.rightCount())
			nm.setKey(nr.key())
			nm.setValue(nr.value())

			nm.setLeftChild(newLeft)
			nc, err := Count(s, newLeft)
			if err != nil {
				return err
			}

			nm.setLeftCount(nc)

			if nr.err() != nil {
				return nr.err()
			}

			return nm.err()
		})

	case 1:
		newRight, err := Delete(s, nr.rightChild(), key)
		if err != nil {
			return store.NilAddress, err
		}

		return s.Append(0,func(f store.Segment) error {
			nm := newNodeModifier(f)
			nm.setLeftChild(nr.leftChild())
			nm.setLeftCount(nr.leftCount())
			nm.setKey(nr.key())
			nm.setValue(nr.value())

			nm.setRightChild(newRight)

			nc, err := Count(s, newRight)
			if err != nil {
				return err
			}

			nm.setRightCount(nc)

			if nr.err() != nil {
				return nr.err()
			}

			return nm.err()
		})

	default:
		return store.NilAddress, errors.New("should never be reached")
	}

}

func findSuccessor(s store.Store, k store.Address) (store.Address, error) {
	nr := newNodeReader(s, k)
	lc := nr.leftChild()
	if lc == store.NilAddress {
		return k, nr.err()
	}

	if nr.err() != nil {
		return store.NilAddress, nr.err()
	}

	return findSuccessor(s, lc)
}
