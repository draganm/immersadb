package trie

import (
	"bytes"
	"sort"

	"github.com/draganm/immersadb/store"
)

func (t *TrieNode) isLeaf() bool {
	for i, c := range t.children {
		lc := t.loadedChildren[i]
		if lc != nil {
			return false
		}
		if c != store.NilAddress {
			return false
		}
	}
	return true
}

func (t *TrieNode) Put(path [][]byte, value store.Address) {
	if !t.isLeaf() {
		panic("putting in non-leafs is not supported yet")
	}

	if len(path) != 1 {
		panic("putting in sub-tries is not supported yet")
	}

	k := path[0]

	idx := sort.Search(len(t.kv), func(i int) bool {
		return bytes.Compare(t.kv[i].key, k) >= 0
	})

	if idx >= len(t.kv) {
		t.count++
	}

	t.kv = append(t.kv[:idx], append([]kvpair{kvpair{k, value}}, t.kv[idx:]...)...)

	t.persistedAddress = nil

}
