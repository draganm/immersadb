package trie

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func (t *trie) persist() (store.Address, error) {
	if t == nil {
		return store.NilAddress, nil
	}

	chaddr := make([]store.Address, 256)
	nnch := 0
	for i, ch := range t.children {
		cha, err := ch.persist()
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while persisting a child")
		}
		chaddr[i] = cha
		if cha != store.NilAddress {
			nnch++
		}
	}

	nch := nnch

	if t.value != store.NilAddress {
		nch++
	}

	sw, err := t.store.CreateSegment(0, store.TypeTrieNode, nch, 1+nnch+len(t.prefix))
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating new segment")
	}

	d := sw.Data

	d[0] = byte(nnch)
	d = d[1:]

	chi := 0

	for i, ch := range chaddr {
		if ch != store.NilAddress {
			sw.SetChild(chi, ch)
			chi++
			d[0] = byte(i)
			d = d[1:]
		}
	}

	if t.value != store.NilAddress {
		sw.SetChild(nnch, t.value)
	}

	copy(d, t.prefix)

	return sw.Address, nil
}

func (t *trie) load() error {

	if t == nil {
		return nil
	}

	if t.address == store.NilAddress {
		return nil
	}

	t.children = make([]*trie, 256)

	sr := t.store.GetSegment(t.address)

	d := sr.GetData()

	nch := int(d[0])

	d = d[1:]

	for i := 0; i < nch; i++ {
		chi := int(d[0])
		t.children[chi] = newPersistedTrie(t.store, sr.GetChildAddress(i))
		d = d[1:]
	}

	t.prefix = make([]byte, len(d))
	copy(t.prefix, d)

	if sr.NumberOfChildren() > nch {
		t.value = sr.GetChildAddress(nch)
	}

	t.address = store.NilAddress

	return nil

}

func newPersistedTrie(s store.Store, a store.Address) *trie {
	return &trie{
		store:   s,
		address: a,
	}
}
