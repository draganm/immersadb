package trie

import (
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

	if len(kp) == 0 {
		// key shares prefix with this node and is shorter
		// insert a new parent with the value
		// add current node as a child

		nch := &trie{
			count:    t.count,
			children: t.children,
			prefix:   pp[1:],
			value:    t.value,
		}

		t.children = make([]*trie, 256)
		t.count++
		t.children[pp[0]] = nch
		t.prefix = cp
		t.value = value
		return true
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

func (t *trie) numberOfChildren() int {
	cc := 0
	for _, c := range t.children {
		if c != nil {
			cc++
		}
	}

	return cc
}

func (t *trie) firstNonNilChild() (int, *trie) {
	for i, c := range t.children {
		if c != nil {
			return i, c
		}
	}
	return -1, nil
}

func (t *trie) delete(key []byte) error {

	if t == nil {
		return ErrNotFound
	}

	cp, kp, pp := commonPrefix(key, t.prefix)
	cp = cp

	if len(kp) == 0 && len(pp) == 0 {
		if t.value != store.NilAddress {
			t.count--
			t.value = store.NilAddress

			// if there is only one child, collapse it to the parent
			if t.numberOfChildren() == 1 {
				i, ch := t.firstNonNilChild()
				t.prefix = append(append(t.prefix, byte(i)), ch.prefix...)
				t.children = ch.children
				t.value = ch.value
			}
			return nil
		}
		return ErrNotFound
	}

	if len(pp) == 0 {
		// key is longer, use child for lookup
		ch := t.children[kp[0]]
		err := ch.delete(kp[1:])
		if err != nil {
			return err
		}
		t.count--

		if ch.emtpy() {
			t.children[kp[0]] = nil
		}

		return nil

	}

	return errors.New("not yet implemented")
}
