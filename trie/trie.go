package trie

import (
	serrors "errors"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

var ErrNotFound = serrors.New("not found")

type trie struct {
	count    uint64
	prefix   []byte
	value    store.Address
	children []*trie

	store   store.Store
	address store.Address
}

func newEmptyTrie(s store.Store) *trie {
	return &trie{
		value:    store.NilAddress,
		children: make([]*trie, 256),
		address:  store.NilAddress,
		store:    s,
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
		ch = newEmptyTrie(t.store)
		t.children[k] = ch
	}
	return ch
}

func (t *trie) insert(key []byte, value store.Address) (bool, error) {

	err := t.load()
	if err != nil {
		return false, err
	}

	if t.emtpy() {
		t.prefix = key
		t.count = 1
		t.value = value
		return true, nil
	}

	cp, kp, pp := commonPrefix(key, t.prefix)

	if len(kp) == 0 && len(pp) == 0 {
		// key and prefix are the same
		if t.value == store.NilAddress {
			t.count++
			t.value = value
			t.address = store.NilAddress
			return true, nil
		}
		t.value = value
		return false, nil
	}

	if len(pp) == 0 {
		// key shares prefix with this node and is longer
		// find or create a child
		// insert into the child the rest of the postfix (kp)
		ch := t.getOrCreateNewChild(kp[0])
		inserted, err := ch.insert(kp[1:], value)
		if err != nil {
			return false, nil
		}

		if inserted {
			t.count++
		}
		return inserted, nil
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
			address:  store.NilAddress,
			store:    t.store,
		}

		t.children = make([]*trie, 256)
		t.count++
		t.children[pp[0]] = nch
		t.prefix = cp
		t.value = value
		t.address = store.NilAddress
		return true, nil
	}

	// branching off scenario - insert a new common parent

	nvc := newEmptyTrie(t.store)
	_, err = nvc.insert(kp[1:], value)
	if err != nil {
		return false, errors.Wrap(err, "while inserting into child")
	}

	ntc := &trie{
		count:    t.count,
		children: t.children,
		prefix:   pp[1:],
		value:    t.value,

		address: store.NilAddress,
		store:   t.store,
	}

	t.children = make([]*trie, 256)
	t.count++
	t.children[pp[0]] = ntc
	t.children[kp[0]] = nvc
	t.prefix = cp
	t.value = store.NilAddress
	return true, nil

}

func (t *trie) get(key []byte) (store.Address, error) {

	err := t.load()
	if err != nil {
		return store.NilAddress, err
	}

	if t == nil {
		return store.NilAddress, ErrNotFound
	}

	_, kp, pp := commonPrefix(key, t.prefix)

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

	return store.NilAddress, ErrNotFound
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

	_, kp, pp := commonPrefix(key, t.prefix)

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

	return ErrNotFound
}
