package trie

import (
	"bytes"
	"sort"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
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

func (t *TrieNode) Put(path [][]byte, value store.Address, valueTrie *TrieNode) error {

	if len(path) == 0 {
		return errors.New("empty path is not supported")
	}

	if value == store.NilAddress && valueTrie == nil {
		return errors.New("trie can't set both nil segment address and nil valueTrie as value")
	}

	if value != store.NilAddress && valueTrie != nil {
		return errors.New("trie can't set both non-nil segment address and non-nil valueTrie as value")
	}

	// isLastPathElement := len(path) == 1

	// if !isLastPathElement {
	// 	return errors.New("putting in sub-tries is not supported yet")
	// }

	k := path[0]

	if !t.isLeaf() {
		cp, ks, ps := commonPrefix(k, t.prefix)
		// case 1: key == prefix - set the value
		if len(ks) == 0 && len(ps) == 0 {
			if t.value == store.NilAddress && t.valueTrie == nil {
				t.count++
			}
			if t.value != value || t.valueTrie != valueTrie {
				t.persistedAddress = nil
			}
			t.value = value
			t.valueTrie = valueTrie
			if t.value != store.NilAddress {
				vt, err := Load(t.store, t.value)
				if err != nil {
					return err
				}
				t.valueTrie = vt
			}
			if t.valueTrie == nil {
				return ErrNotFound
			}

			return t.valueTrie.Put(path[1:], value, valueTrie)
		}

		// case 2: common prefix == prefix - delegate to the child, create if necessary
		if len(cp) == len(t.prefix) {
			splitByte := ks[0]
			ch, err := t.loadOrCreateEmptyChild(splitByte)
			if err != nil {
				return err
			}

			path[0] = ks[1:]
			ch.Put(path, value, valueTrie)
			t.persistedAddress = nil
			t.count++
			return nil
		}

		// case 3: common prefix != prefix - insert an intermediate node
		ch := t.copy()

		ch.persistedAddress = nil
		ch.prefix = ps[1:]

		for i := range t.children {
			t.children[i] = store.NilAddress
			t.loadedChildren[i] = nil
		}

		t.kv = nil
		t.valueTrie = nil
		t.value = value
		t.prefix = cp

		splitByte := ps[0]
		t.loadedChildren[splitByte] = ch

		nch := NewEmpty(t.store)
		path[0] = ks[1:]
		nch.Put(path, value, valueTrie)
		t.loadedChildren[ks[0]] = nch
		t.count++
		t.persistedAddress = nil
		return nil

	}

	idx := sort.Search(len(t.kv), func(i int) bool {
		return bytes.Compare(t.kv[i].key, k) >= 0
	})

	if idx < len(t.kv) && bytes.Compare(k, t.kv[idx].key) == 0 {
		kv := t.kv[idx]
		if kv.value == value && kv.valueTrie == valueTrie {
			return nil
		}
		t.kv[idx].value = value
		t.kv[idx].valueTrie = valueTrie
		t.persistedAddress = nil
		return nil
	}

	t.kv = append(t.kv[:idx], append([]kvpair{kvpair{k, value, valueTrie}}, t.kv[idx:]...)...)

	if len(t.kv) > maxLeafSize {
		// find longest common prefix
		cp := t.kv.longestCommonPrefix()
		if bytes.Equal(cp, t.kv[0].key) {
			// TODO implement this!
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
			cpth := [][]byte{kv.key[len(cp)+1:]}
			ch.Put(cpth, kv.value, kv.valueTrie)
		}

		t.kv = nil
		t.prefix = cp

		// insert common node with the prefix
		// common node count adds one
	}

	t.count++
	t.persistedAddress = nil

	return nil

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

func (t *TrieNode) loadOrCreateEmptyChild(idx byte) (*TrieNode, error) {
	loaded := t.loadedChildren[idx]
	if loaded != nil {
		return loaded, nil
	}

	if t.children[idx] == store.NilAddress {
		loaded = NewEmpty(t.store)
		t.loadedChildren[idx] = loaded
		return loaded, nil
	}

	loaded, err := Load(t.store, t.children[idx])
	if err != nil {
		return nil, errors.Wrapf(err, "while loading child %d", idx)
	}
	t.loadedChildren[idx] = loaded

	return loaded, nil
}
