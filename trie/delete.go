package trie

import (
	"bytes"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func Delete(s store.Store, root store.Address, key []byte) (store.Address, error) {
	nr, err := delete(s, root, key)
	if err != nil {
		return store.NilAddress, err
	}

	if nr == store.NilAddress {
		return CreateEmpty(s)
	}

	return nr, nil
}

func delete(s store.Store, root store.Address, key []byte) (store.Address, error) {
	f, err := s.Get(root)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting trie fragment")
	}

	tm := NewTrieModifier(f)
	prefix := tm.GetPrefix()

	if bytes.Equal(prefix, key) {
		noChildren := true
		for i := 0; i < 256; i++ {
			noChildren = noChildren && tm.GetChild(i) == store.NilAddress
		}

		if tm.Error() != nil {
			return store.NilAddress, tm.Error()
		}

		if noChildren {
			return store.NilAddress, nil
		}

		return createTrieNodeCopy(s, f, func(f TrieModifier) error {
			f.SetChild(256, store.NilAddress)
			return f.Error()
		})
	}

	_, kr, pr := commonPrefix(key, prefix)

	if len(pr) != 0 {
		return store.NilAddress, ErrNotFound
	}

	idx := int(kr[0])

	chKey := tm.GetChild(idx)

	if tm.Error() != nil {
		return store.NilAddress, tm.Error()
	}

	return createTrieNodeCopy(s, f, func(f TrieModifier) error {
		nck, err := delete(s, chKey, kr[1:])
		if err != nil {
			f.SetError(err)
		}
		f.SetChild(idx, nck)
		return f.Error()
	})

}
