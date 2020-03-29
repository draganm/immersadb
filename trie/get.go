package trie

import (
	"bytes"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("not found")

func Get(s store.Store, root store.Address, key []byte) (store.Address, error) {
	sr := s.GetSegment(root)

	tr := trie(sr)

	prefix := tr.getPrefix()

	if bytes.Equal(prefix, key) {
		v := tr.getValue()

		if v == store.NilAddress {
			return store.NilAddress, ErrNotFound
		}

		return v, nil
	}

	_, kr, pr := commonPrefix(key, prefix)

	if len(pr) != 0 {
		return store.NilAddress, ErrNotFound
	}

	chKey := tr.getChild(kr[0])

	if chKey == store.NilAddress {
		return store.NilAddress, ErrNotFound
	}

	return Get(s, chKey, kr[1:])

}
