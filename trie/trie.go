package trie

import (
	"encoding/binary"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type trie store.SegmentReader

func (t trie) getChild(i byte) store.Address {
	sr := store.SegmentReader(t)
	d := sr.GetData()

	nch := d[8]
	for _, chi := range d[9 : nch+9] {
		if chi == i {
			return sr.GetChildAddress(int(chi))
		}
		if chi > i {
			return store.NilAddress
		}
	}
	return store.NilAddress
}

func (t trie) count() uint64 {
	sr := store.SegmentReader(t)
	d := sr.GetData()
	return binary.BigEndian.Uint64(d)
}

func (t trie) getValue() store.Address {
	sr := store.SegmentReader(t)
	d := sr.GetData()
	nch := d[8]
	if sr.NumberOfChildren() > int(nch) {
		return sr.GetChildAddress(int(nch) + 1)
	}
	return store.NilAddress
}

func (t trie) getPrefix() []byte {
	sr := store.SegmentReader(t)
	d := sr.GetData()
	nch := d[8]
	return d[9+nch:]
}

type newTrieNode struct {
	count    uint64
	prefix   []byte
	children []store.Address
	value    store.Address
}

func trieNodeCopy(t trie) *newTrieNode {
	ntn := &newTrieNode{
		count:    t.count(),
		prefix:   t.getPrefix(),
		value:    t.getValue(),
		children: make([]store.Address, 256),
	}

	for i := range ntn.children {
		ntn.children[i] = t.getChild(i)
	}

	return ntn

}

func emptyNewTrieNode() *newTrieNode {
	ntn := &newTrieNode{
		children: make([]store.Address, 256),
	}

	for i := 0; i < 256; i++ {
		ntn.children[i] = store.NilAddress
	}

	return ntn
}

func (n *newTrieNode) store(s store.Store, layer int) (store.Address, error) {
	nch := 0

	for _, ch := range n.children {
		if ch != store.NilAddress {
			nch++
		}
	}

	tnc := nch

	if n.value != store.NilAddress {
		tnc++
	}

	sw, err := s.CreateSegment(layer, store.TypeTrieNode, tnc, 1+nch+len(n.prefix))
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating new trie segment")
	}

	d := sw.Data

	d[0] = byte(nch)
	d = d[1:]

	chi := 0

	for i, ch := range n.children {
		if ch != store.NilAddress {
			d[0] = byte(i)
			d = d[1:]
			sw.SetChild(chi, ch)
			chi++
		}
	}

	copy(d, n.prefix)

	return sw.Address, nil

}
