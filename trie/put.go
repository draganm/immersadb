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

	if value == store.NilAddress {
		panic("trie can't set nil segment as value")
	}

	if len(path) != 1 {
		panic("putting in sub-tries is not supported yet")
	}

	k := path[0]

	if !t.isLeaf() {
		cp, ks, ps := commonPrefix(k, t.prefix)
		// case 1: key == prefix - set the value
		if len(ks) == 0 && len(ps) == 0 {
			if t.value != value {
				t.value = value
				t.persistedAddress = nil
				t.count++
			}
			return
		}

		// case 2: common prefix == prefix - delegate to the child, create if necessary
		if len(cp) == len(t.prefix) {
			splitByte := ks[0]
			ch := t.loadOrCreateEmptyChild(splitByte)
			path[0] = ks[1:]
			ch.Put(path, value)
			t.persistedAddress = nil
			t.count++
			return
		}

		// case 3: common prefix != prefix - insert an intermediate node
		ch := t.copy()

		// TODO: this case is incomplete!

		ch.persistedAddress = nil
		ch.prefix = ps[1:]

		for i := range t.children {
			t.children[i] = store.NilAddress
			t.loadedChildren[i] = nil
		}

		t.kv = nil
		t.kvTries = nil
		t.valueTrie = nil
		t.value = value
		t.prefix = cp

		splitByte := ps[0]
		t.loadedChildren[splitByte] = ch
		t.count++
		t.persistedAddress = nil
		return

	}

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

func commonPrefix(p1, p2 []byte) ([]byte, []byte, []byte) {

	maxIndex := len(p1)
	if len(p2) < maxIndex {
		maxIndex = len(p2)
	}

	for i := 0; i < maxIndex; i++ {
		if p1[i] != p2[i] {
			return p1[:i], p1[i:], p2[i:]
		}
	}

	return p1[:maxIndex], p1[maxIndex:], p2[maxIndex:]
}

func (t *TrieNode) loadOrCreateEmptyChild(idx byte) *TrieNode {
	loaded := t.loadedChildren[idx]
	if loaded != nil {
		return loaded
	}

	if t.children[idx] == store.NilAddress {
		loaded = NewEmpty(t.store)
		t.loadedChildren[idx] = loaded
		return loaded
	}

	loaded = Load(t.store, t.children[idx])
	t.loadedChildren[idx] = loaded

	return loaded
}
