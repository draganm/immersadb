package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func ForEach(s store.Store, root store.Address, f func([]byte, store.Address) error) error {
	if root == store.NilAddress {
		return nil
	}

	nr, err := newNodeReader(s, root)
	if err != nil {
		return errors.Wrap(err, "while creating node reader")
	}

	lc := nr.leftChild()
	rc := nr.rightChild()
	k := nr.key()
	v := nr.value()

	err = ForEach(s, lc, f)
	if err != nil {
		return err
	}

	err = f(k, v)
	if err != nil {
		return err
	}

	return ForEach(s, rc, f)

}
