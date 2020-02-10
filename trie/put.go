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

const maxLeafSize = 32

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

	if idx < len(t.kv) && bytes.Compare(k, t.kv[idx].key) == 0 {
		if t.kv[idx].value == value {
			return
		}
		t.kv[idx].value = value
		return
	}

	t.kv = append(t.kv[:idx], append([]kvpair{kvpair{k, value}}, t.kv[idx:]...)...)

	if len(t.kv) > maxLeafSize {
		// find longest common prefix
		cp := t.kv.longestCommonPrefix()
		if bytes.Equal(cp, t.kv[0].key) {
			// special case - first key is same as longest prefix
			panic("not yet implemented")
		}
		// split to leaves - remove prefix
		for _, kv := range t.kv {
			splitByte := int(kv.key[len(cp)])
			ch := t.loadedChildren[splitByte]
			if ch == nil {
				ch = NewEmpty(t.store)
				t.loadedChildren[splitByte] = ch
			}
			ch.Put([][]byte{kv.key[len(cp)+1:]}, kv.value)
		}

		t.kv = nil
		t.prefix = cp

		// insert common node with the prefix
		// common node count adds one
	}

	t.count++
	t.persistedAddress = nil

}
