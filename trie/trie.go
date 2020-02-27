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

func (t *trie) getOrCreateNewChild(k byte) *trie {
	ch := t.children[k]
	if ch == nil {
		ch = newEmptyTrie()
		t.children[k] = ch
	}
	return ch
}

func (t *trie) insert(key []byte, value store.Address) bool {
	if t.emtpy() {
		t.prefix = key
		t.count = 1
		t.value = value
		return true
	}

	cp, kp, pp := commonPrefix(key, t.prefix)
	cp = cp

	if len(kp) == 0 && len(pp) == 0 {
		// key and prefix are the same
		if t.value == store.NilAddress {
			t.count++
			t.value = value
			return true
		}
		t.value = value
		return false
	}

	if len(pp) == 0 {
		// key shares prefix with this node and is longer
		// find or create a child
		// insert into the child the rest of the postfix (kp)
		ch := t.getOrCreateNewChild(kp[0])
		inserted := ch.insert(kp[1:], value)
		if inserted {
			t.count++
		}
		return inserted
	}

	panic("not yet implemented")
}

func (t *trie) get(key []byte) (store.Address, error) {

	if t == nil {
		return store.NilAddress, ErrNotFound
	}

	cp, kp, pp := commonPrefix(key, t.prefix)
	cp = cp

	if len(kp) == 0 && len(pp) == 0 {
		if t.value != store.NilAddress {
			return t.value, nil
		}
		return store.NilAddress, ErrNotFound
	}

	if len(pp) == 0 {
		// key is longer, use child for lookup
		ch := t.children[kp[0]]
		return ch.get(kp[1:])
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
