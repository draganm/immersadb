package trie

import (
	"bytes"
	"errors"
	serrors "errors"

	"github.com/draganm/immersadb/store"
)

var ErrNotFound = serrors.New("not found")

func (t *TrieNode) Get(path [][]byte) (store.Address, error) {
	if !t.isLeaf() {
		return store.NilAddress, errors.New("non leaf gets are not yet supported")
	}
	for _, kv := range t.kv {
		if bytes.Compare(kv.key, path[0]) == 0 {
			return kv.value, nil
		}
	}
	return store.NilAddress, ErrNotFound
}
