package trie

import (
	"bytes"
	"errors"
	serrors "errors"
	"sort"

	"github.com/draganm/immersadb/store"
)

var ErrNotFound = serrors.New("not found")

func (t *TrieNode) Get(path [][]byte) (store.Address, error) {
	if !t.isLeaf() {
		return store.NilAddress, errors.New("non leaf gets are not yet supported")
	}

	k := path[0]

	idx := sort.Search(len(t.kv), func(i int) bool {
		return bytes.Compare(t.kv[i].key, k) >= 0
	})

	if idx < len(t.kv) {
		return t.kv[idx].value, nil
	}

	return store.NilAddress, ErrNotFound
}
