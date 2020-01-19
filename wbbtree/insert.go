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
		return s.Append(0,func(f store.Segment) error {
			nm := newNodeModifier(f)
			nm.setKey(key)
			nm.setValue(value)
			return nm.err()
		})
	}

	nr := newNodeReader(s, root)

	cmp := bytes.Compare(key, nr.key())

	switch cmp {
	case 0:
		return s.Append(0,func(f store.Segment) error {
			nm := newNodeModifier(f)
			nm.setLeftChild(nr.leftChild())
			nm.setRightChild(nr.rightChild())
			nm.setLeftCount(nr.leftCount())
			nm.setRightCount(nr.rightCount())
			nm.setKey(nr.key())

			nm.setValue(value)

			if nr.err() != nil {
				return nr.err()
			}

			return nm.err()
		})

	case -1:
		newLeft, err := Insert(s, nr.leftChild(), key, value)
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
		newRight, err := Insert(s, nr.rightChild(), key, value)
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
