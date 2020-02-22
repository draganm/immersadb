package trie

import (
	"encoding/binary"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type kvpair struct {
	key   []byte
	value *TrieNode
}

type kvpairSlice []kvpair

func (s kvpairSlice) longestCommonPrefix() []byte {

	if len(s) == 0 {
		return nil
	}
	firstKey := s[0].key

	for i := 0; true; i++ {

		if len(firstKey) < i {
			return firstKey
		}

		for _, kvp := range s[1:] {
			k := kvp.key
			if len(k) < i {
				return firstKey[:i]
			}
			if k[i] != firstKey[i] {
				return firstKey[:i]
			}
		}
	}

	return firstKey
}

type TrieNode struct {
	loaded           bool
	modified         bool
	persistedAddress store.Address
	// children         []store.Address
	children []*TrieNode
	count    uint64
	prefix   []byte
	value    *TrieNode
	store    store.Store
	kv       kvpairSlice
}

func NewLazyTrieNode(store store.Store, address store.Address) *TrieNode {
	return &TrieNode{
		persistedAddress: address,
		store:            store,
	}
}

func (t *TrieNode) load() error {
	if t.loaded {
		return nil
	}

	sr := t.store.GetSegment(t.persistedAddress)

	if sr.Type() != store.TypeTrieNode {
		return errors.Errorf("tried to load node with type %s as %s node", sr.Type(), store.TypeTrieNode)
	}

	children := make([]store.Address, 256)
	for i := range children {
		children[i] = store.NilAddress
	}

	loadedChildren := make([]*TrieNode, 256)

	d := sr.GetData()

	if len(d) < 9 {
		return errors.New("trie node segment data is less than 9 bytes")
	}

	count := binary.BigEndian.Uint64(d[:8])

	nch := int(d[8])

	for i, chm := range d[8+1 : 8+1+nch] {
		children[chm] = sr.GetChildAddress(i)
	}

	// TODO check data size

	prefixLength := int(binary.BigEndian.Uint16(d[8+1+nch : 8+1+nch+2]))
	prefix := d[8+1+nch+2 : 8+1+nch+2+prefixLength]

	chindex := nch
	kv := []kvpair{}
	value := sr.GetChildAddress(chindex)
	chindex++

	// TODO check data size

	kvd := d[8+1+nch+2+prefixLength:]

	for len(kvd) > 0 {
		len := int(binary.BigEndian.Uint16(kvd[:2]))
		k := kvd[2 : 2+len]
		// kv[string(k)] = sr.GetChildAddress(chindex)
		kv = append(kv, kvpair{k, NewLazyTrieNode(t.store, sr.GetChildAddress(chindex))})

		chindex++
		kvd = kvd[2+len:]
	}

	t.children = children

	return nil

	return &TrieNode{
		persistedAddress: &ad,
		children:         children,
		loadedChildren:   loadedChildren,
		count:            count,
		prefix:           prefix,
		store:            st,
		value:            value,
		kv:               kv,
	}, nil

	Load(t.store)
}

func (t *TrieNode) copy() *TrieNode {
	cp := &TrieNode{
		persistedAddress: t.persistedAddress,
		count:            t.count,
		value:            t.value,
		store:            t.store,
		valueTrie:        t.valueTrie,
		prefix:           t.prefix,
	}

	for _, ch := range t.children {
		cp.children = append(cp.children, ch)
	}

	for _, lc := range t.loadedChildren {
		cp.loadedChildren = append(cp.loadedChildren, lc)
	}

	for _, kv := range t.kv {
		cp.kv = append(cp.kv, kv)
	}

	return cp

}

// Layout
// 8 bytes count
// byte childrenCount
// bytes childrenCount mapping byte -> child index
// 2 bytes prefix length
// prefix length bytes prefix
// len/data key

func Load(st store.Store, ad store.Address) (*TrieNode, error) {
	sr := st.GetSegment(ad)

	if sr.Type() != store.TypeTrieNode {
		return nil, errors.Errorf("tried to load node with type %s as %s node", sr.Type(), store.TypeTrieNode)
	}

	children := make([]store.Address, 256)
	for i := range children {
		children[i] = store.NilAddress
	}

	loadedChildren := make([]*TrieNode, 256)

	d := sr.GetData()

	if len(d) < 9 {
		return nil, errors.New("trie node segment data is less than 9 bytes")
	}

	count := binary.BigEndian.Uint64(d[:8])

	nch := int(d[8])

	for i, chm := range d[8+1 : 8+1+nch] {
		children[chm] = sr.GetChildAddress(i)
	}

	// TODO check data size

	prefixLength := int(binary.BigEndian.Uint16(d[8+1+nch : 8+1+nch+2]))
	prefix := d[8+1+nch+2 : 8+1+nch+2+prefixLength]

	chindex := nch
	kv := []kvpair{}
	value := sr.GetChildAddress(chindex)
	chindex++

	// TODO check data size

	kvd := d[8+1+nch+2+prefixLength:]

	for len(kvd) > 0 {
		len := int(binary.BigEndian.Uint16(kvd[:2]))
		k := kvd[2 : 2+len]
		// kv[string(k)] = sr.GetChildAddress(chindex)
		kv = append(kv, kvpair{k, sr.GetChildAddress(chindex), nil})
		chindex++
		kvd = kvd[2+len:]
	}

	return &TrieNode{
		persistedAddress: &ad,
		children:         children,
		loadedChildren:   loadedChildren,
		count:            count,
		prefix:           prefix,
		store:            st,
		value:            value,
		kv:               kv,
	}, nil
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

	for _, vt := range t.kv {
		if !vt.valueTrie.isPersisted() {
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
		pa, err := lc.Persist()
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while getting child's persisted address")
		}
		t.children[i] = pa
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

	for i, vt := range t.kv {
		if vt.valueTrie != nil && !vt.valueTrie.isPersisted() {
			vta, err := vt.valueTrie.Persist()
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
		if ch != store.NilAddress {
			sw.SetChild(chindex, ch)
			chindex++
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
		value: store.NilAddress,
	}

}

func (t *TrieNode) Count(path [][]byte) uint64 {
	if len(path) == 0 {
		return t.count
	}

}
