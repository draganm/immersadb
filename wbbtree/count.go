package wbbtree

import (
	"github.com/draganm/immersadb/store"
)

func Count(f store.Store, root store.Address) (uint64, error) {
	if root == store.NilAddress {
		return 0, nil
	}

	nr := newNodeReader(f, root)
	return nr.leftCount() + nr.rightCount() + 1, nr.err()
}
