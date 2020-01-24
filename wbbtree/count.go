package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func Count(f store.Store, root store.Address) (uint64, error) {
	if root == store.NilAddress {
		return 0, nil
	}

	nr, err := newNodeReader(f, root)
	if err != nil {
		return 0, errors.Wrap(err, "while creating node reader")
	}

	if nr.isEmpty() {
		return 0, nil
	}

	return nr.leftCount() + nr.rightCount() + 1, nil
}
