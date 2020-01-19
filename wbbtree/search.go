package wbbtree

import (
	"bytes"	
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"

	serrors "errors"
)

var ErrNotFound = serrors.New("Not found")

func Search(s store.Store, root store.Address, key []byte) (store.Address, error) {
	if root == store.NilAddress {
		return store.NilAddress, ErrNotFound
	}

	nr := newNodeReader(s, root)

	cmp := bytes.Compare(key, nr.key())

	if cmp == 0 {
		return nr.value(), nr.err()
	}

	switch cmp {
	case 0:
		return nr.value(), nr.err()
	case -1:
		return Search(s, nr.leftChild(), key)
	case 1:
		return Search(s, nr.rightChild(), key)
	default:
		return store.NilAddress, errors.New("should never be reached")
	}

}
