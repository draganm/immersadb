package trie

import (
	"bytes"
	serrors "errors"
	"sort"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

var ErrNotFound = serrors.New("not found")

func (t *TrieNode) loadedChild(idx byte) (*TrieNode, error) {
	loaded := t.loadedChildren[idx]
	if loaded != nil {
		return loaded, nil
	}

	if t.children[idx] == store.NilAddress {
		return nil, nil
	}

	loaded, err := Load(t.store, t.children[idx])
	if err != nil {
		return nil, errors.Wrapf(err, "while loading child %d", idx)
	}
	t.loadedChildren[idx] = loaded

	return loaded, nil
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

		lc, err := t.loadedChild(splitByte)
		if err != nil {
			return store.NilAddress, err
		}

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
