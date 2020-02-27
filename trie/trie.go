package trie

import (
	"bytes"
	"errors"

	"github.com/draganm/immersadb/store"
)

var ErrNotFound = errors.New("not found")

type trie struct {
	count    uint64
	prefix   []byte
	value    store.Address
	children []*trie
}

func newEmptyTrie() *trie {
	return &trie{
		children: make([]*trie, 256),
	}
}

func (t *trie) emtpy() bool {
	for _, c := range t.children {
		if c != nil {
			return false
		}
	}
	return t.count == 0
}

func (t *trie) insert(key []byte, value store.Address) {
	if t.emtpy() {
		t.prefix = key
		t.count = 1
		return
	}

	panic("not yet implemented")
}

func (t *trie) get(key []byte) (store.Address, error) {
	if bytes.Compare(key, t.prefix) == 0 {
		if t.value != store.NilAddress {
			return t.value, nil
		}
		return store.NilAddress, ErrNotFound
	}

	return store.NilAddress, errors.New("not yet implemented")
}

func (t *trie) delete(key []byte) error {
	if bytes.Compare(key, t.prefix) == 0 {

		if t.value != store.NilAddress {
			t.count--
			t.value = store.NilAddress
			return nil
		}
		return ErrNotFound
	}

	return errors.New("not yet implemented")
}
