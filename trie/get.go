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
		if len(k) < len(t.prefix) {
			return store.NilAddress, ErrNotFound
		}

		kpref := k[:len(t.prefix)]

		if bytes.Equal(kpref, t.prefix) {
			if len(kpref) == len(k) {
				if t.value != store.NilAddress {
					return t.value, nil
				}
				return store.NilAddress, ErrNotFound
			}

			splitByte := k[len(kpref)]

			lc := t.loadedChild(splitByte)

			postf := k[len(kpref)+1:]
			path[0] = postf

			return lc.Get(path)

		}
		return store.NilAddress, ErrNotFound
	}

	idx := sort.Search(len(t.kv), func(i int) bool {
		return bytes.Compare(t.kv[i].key, k) >= 0
	})

	if idx < len(t.kv) {
		return t.kv[idx].value, nil
	}

	return store.NilAddress, ErrNotFound
}
