package wbbtree

import (
	"github.com/draganm/immersadb/store"
)

func ForEach(s store.Store, root store.Address, f func([]byte, store.Address) error) error {
	if root == store.NilAddress {
		return nil
	}

	nr := newNodeReader(s, root)
	lc := nr.leftChild()
	rc := nr.rightChild()
	k := nr.key()
	v := nr.value()

	if nr.err() != nil {
		return nr.err()
	}

	err := ForEach(s, lc, f)
	if err != nil {
		return err
	}

	err = f(k, v)
	if err != nil {
		return err
	}

	return ForEach(s, rc, f)

}
