package trie

import "github.com/draganm/immersadb/store"

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

	t.kv = append(t.kv, kvpair{
		key:   path[0],
		value: value,
	})

	t.count++

	t.persistedAddress = nil

}
