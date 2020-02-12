package trie

import (
	"bytes"
	serrors "errors"
	"sort"

	"github.com/draganm/immersadb/store"
)

var ErrNotFound = serrors.New("not found")

func (t *TrieNode) loadedChild(idx byte) *TrieNode {
	loaded := t.loadedChildren[idx]
	if loaded != nil {
		return loaded
	}

	if t.children[idx] == store.NilAddress {
		return nil
	}

	loaded = Load(t.store, t.children[idx])
	t.loadedChildren[idx] = loaded

	return loaded
}

func (t *TrieNode) Get(path [][]byte) (store.Address, error) {
	k := path[0]

	if !t.isLeaf() {

		_, kp, pp := commonPrefix(k, t.prefix)

		if len(pp) > 0 {
			return store.NilAddress, ErrNotFound
		}

		if len(kp) == 0 {
			if t.value != store.NilAddress {
				return t.value, nil
			}
			return store.NilAddress, ErrNotFound
		}

		splitByte := kp[0]

		lc := t.loadedChild(splitByte)

		if lc == nil {
			return store.NilAddress, ErrNotFound
		}

		path[0] = kp[1:]

		return lc.Get(path)

	}

	idx := sort.Search(len(t.kv), func(i int) bool {
		return bytes.Compare(t.kv[i].key, k) >= 0
	})

	if idx < len(t.kv) {
		return t.kv[idx].value, nil
	}

	return store.NilAddress, ErrNotFound
}
