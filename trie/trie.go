package trie

import (
	"encoding/binary"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type kvpair struct {
	key   []byte
	value store.Address
}

type kTrieNode struct {
	key      []byte
	trieNode *TrieNode
}

type TrieNode struct {
	persistedAddress *store.Address
	kv               []kvpair
	children         []store.Address
	loadedChildren   []*TrieNode
	count            uint64
	prefix           string
	value            store.Address
	store            store.Store
	valueTrie        *TrieNode
	kvTries          []kTrieNode
}

// Layout
// 8 bytes count
// byte childrenCount
// bytes childrenCount mapping byte -> child index
// 2 bytes prefix length
// prefix length bytes prefix
// len/data key

func Load(st store.Store, ad store.Address) *TrieNode {
	sr := st.GetSegment(ad)

	children := make([]store.Address, 256)
	for i := range children {
		children[i] = store.NilAddress
	}

	loadedChildren := make([]*TrieNode, 256)

	d := sr.GetData()

	count := binary.BigEndian.Uint64(d[:8])

	nch := int(d[8])

	for i, chm := range d[8+1 : 8+1+nch] {
		children[chm] = sr.GetChildAddress(i)
	}

	prefixLength := int(binary.BigEndian.Uint16(d[1+nch : 1+nch+2]))
	prefix := d[8+1+nch : 8+1+nch+prefixLength]

	chindex := nch
	kv := []kvpair{}
	value := sr.GetChildAddress(chindex)
	chindex++

	kvd := d[8+1+nch+2+prefixLength:]

	for len(kvd) > 0 {
		len := int(binary.BigEndian.Uint16(kvd[:2]))
		k := kvd[2 : 2+len]
		// kv[string(k)] = sr.GetChildAddress(chindex)
		kv = append(kv, kvpair{k, sr.GetChildAddress(chindex)})
		chindex++
		kvd = kvd[2+len:]
	}

	return &TrieNode{
		persistedAddress: &ad,
		children:         children,
		loadedChildren:   loadedChildren,
		count:            count,
		prefix:           string(prefix),
		store:            st,
		value:            value,
		kv:               kv,
	}
}

func (t *TrieNode) isPersisted() bool {
	if t == nil {
		return true
	}

	if t.persistedAddress == nil {
		return false
	}

	for _, lc := range t.loadedChildren {
		if !lc.isPersisted() {
			return false
		}
	}

	if t.valueTrie != nil {
		if !t.valueTrie.isPersisted() {
			return false
		}
	}

	for _, vt := range t.kvTries {
		if !vt.trieNode.isPersisted() {
			return false
		}
	}

	return true
}

func (t *TrieNode) Persist() (store.Address, error) {
	if t == nil {
		return store.NilAddress, nil
	}

	if t.isPersisted() {
		return *t.persistedAddress, nil
	}

	for i, lc := range t.loadedChildren {
		if !lc.isPersisted() {
			pa, err := lc.Persist()
			if err != nil {
				return store.NilAddress, errors.Wrap(err, "while getting child's persisted address")
			}
			t.children[i] = pa
		}
	}

	if t.valueTrie != nil {
		if !t.valueTrie.isPersisted() {
			va, err := t.valueTrie.Persist()
			if err != nil {
				return store.NilAddress, errors.Wrap(err, "while persisting value trie")
			}
			t.value = va
		}
	}

	for i, vt := range t.kvTries {
		if vt.trieNode != nil && !vt.trieNode.isPersisted() {
			vta, err := vt.trieNode.Persist()
			if err != nil {
				return store.NilAddress, errors.Wrapf(err, "while persisting value trie for %q", string(vt.key))
			}
			t.kv[i].value = vta
		}
	}

	nch := 0

	for _, ch := range t.children {
		if ch != store.NilAddress {
			nch++
		}
	}

	segmentSize := 8 + 1 + nch + 2 + len(t.prefix)

	totalChildren := nch + 1 + len(t.kv)

	for _, kv := range t.kv {
		segmentSize += 2 + len(kv.key)
	}

	sw, err := t.store.CreateSegment(0, store.TypeTrieNode, totalChildren, segmentSize)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating segment")
	}

	d := sw.Data

	binary.BigEndian.PutUint64(d, t.count)

	d = d[8:]

	d[0] = byte(nch)
	d = d[1:]
	chindex := 0
	for i, ch := range t.children {
		sw.SetChild(chindex, ch)
		chindex++
		if ch != store.NilAddress {
			d[0] = byte(i)
			d = d[1:]
		}
	}

	binary.BigEndian.PutUint16(d, uint16(len(t.prefix)))
	d = d[2:]
	copy(d, t.prefix)

	sw.SetChild(chindex, t.value)
	chindex++

	d = d[len(t.prefix):]

	for _, kv := range t.kv {
		binary.BigEndian.PutUint16(d, uint16(len(kv.key)))
		d = d[2:]
		copy(d, kv.key)
		d = d[len(kv.key):]
		sw.SetChild(chindex, kv.value)
		chindex++
	}

	t.persistedAddress = &sw.Address

	return *t.persistedAddress, nil
}

func NewEmpty(st store.Store) *TrieNode {

	return &TrieNode{
		store: st,
	}

}

func (t *TrieNode) Count() uint64 {
	return t.count
}
