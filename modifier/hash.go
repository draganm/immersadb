package modifier

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/draganm/immersadb/chunk"
)

var ErrNotHashChunk = errors.New("Not a hash chunk")

func dataToList(data []byte) []string {
	l := []string{}
	for len(data) > 0 {
		len := binary.BigEndian.Uint16(data)
		s := data[2 : 2+len]
		l = append(l, string(s))
		data = data[2+len:]
	}
	return l
}

func listToData(list []string) []byte {
	data := []byte{}
	for _, e := range list {
		ent := make([]byte, 2+len(e))
		binary.BigEndian.PutUint16(ent, uint16(len(e)))
		copy(ent[2:], []byte(e))
		data = append(data, ent...)
	}
	return data
}

func refmapToData(m map[string]uint64) []byte {
	sorted := []string{}
	for k := range m {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	refs := []uint64{}
	for _, k := range sorted {
		refs = append(refs, m[k])
	}

	return chunk.Pack(chunk.HashLeafType, refs, listToData(sorted))
}

func dataToRefmap(c []byte) map[string]uint64 {
	m := map[string]uint64{}
	_, refs, d := chunk.Parts(c)

	names := dataToList(d)

	for i := range refs {
		m[names[i]] = refs[i]
	}

	return m
}

// TODO: handle the pathological case of long common prefix!

func byteOfKey(key string, level int) int {
	if len(key)-1 < level/2 {
		return 0
	}

	b := int([]byte(key)[level/2]) & 0xff

	if level%2 == 1 {
		return b & 0xf
	}

	return b >> 4
}

func (m *Modifier) addToLeafHashWithLevel(hashAddress uint64, key string, addr uint64, level int) (uint64, error) {
	oldChunk := m.Chunk(hashAddress)
	hm := dataToRefmap(oldChunk)

	hm[key] = addr

	if len(hm) > 16 {

		emptyLeafRef, err := m.Append(chunk.Pack(chunk.HashLeafType, nil, nil))
		if err != nil {
			return 0, err
		}

		maps := make([]map[string]uint64, 16)

		for k, addr := range hm {
			fb := byteOfKey(k, level)
			m := maps[fb]
			if m == nil {
				m = map[string]uint64{}
				maps[fb] = m
			}
			m[k] = addr
		}

		refs := make([]uint64, 16)

		for i, mp := range maps {
			if m != nil {
				d := refmapToData(mp)
				r, err := m.Append(d)
				if err != nil {
					return 0, err
				}
				refs[i] = r
			} else {
				refs[i] = emptyLeafRef
			}
		}
		sizeData := make([]byte, 8)
		binary.BigEndian.PutUint64(sizeData, uint64(16+1))
		return m.Append(chunk.Pack(chunk.HashNodeType, refs, sizeData))

	}

	return m.Append(refmapToData(hm))
}

func (m *Modifier) addToNodeHash(hashAddress uint64, key string, addr uint64, level int) (uint64, error) {
	oldChunk := m.Chunk(hashAddress)
	_, refs, sizeData := chunk.Parts(oldChunk)
	b := byteOfKey(key, level) // int([]byte(key)[level]) & 0xff
	subAddr, err := m.addToHashWithLevel(refs[b], key, addr, level+1)
	if err != nil {
		return 0, err
	}
	refs[b] = subAddr

	newSizeData := make([]byte, 8)

	binary.BigEndian.PutUint64(newSizeData, binary.BigEndian.Uint64(sizeData)+1)

	return m.Append(chunk.Pack(chunk.HashNodeType, refs, newSizeData))
}

func (m *Modifier) addToHash(hashAddress uint64, key string, addr uint64) (uint64, error) {
	return m.addToHashWithLevel(hashAddress, key, addr, 0)
}

func (m *Modifier) addToHashWithLevel(hashAddress uint64, key string, addr uint64, level int) (uint64, error) {
	oldChunk := m.Chunk(hashAddress)
	switch chunk.Type(oldChunk) {
	case chunk.HashLeafType:
		return m.addToLeafHashWithLevel(hashAddress, key, addr, level)
	case chunk.HashNodeType:
		return m.addToNodeHash(hashAddress, key, addr, level)
	default:
		return 0, ErrNotHashChunk
	}
}

func (m *Modifier) lookupAddressInHashWithLevel(hashAddress uint64, key string, level int) (uint64, error) {
	c := m.Chunk(hashAddress)
	switch chunk.Type(c) {
	case chunk.HashLeafType:
		hm := dataToRefmap(c)
		addr, found := hm[key]
		if !found {
			return 0, ErrDoesNotExist
		}
		return addr, nil
	case chunk.HashNodeType:
		_, refs, _ := chunk.Parts(c)
		b := byteOfKey(key, level)
		return m.lookupAddressInHashWithLevel(refs[b], key, level+1)
	default:
		return 0, ErrNotHashChunk
	}

}

func (m *Modifier) lookupAddressInHash(hashAddress uint64, key string) (uint64, error) {
	return m.lookupAddressInHashWithLevel(hashAddress, key, 0)
}

func (m *Modifier) forEachHashEntry(hashAddress uint64, f func(string, uint64) error) error {
	c := m.Chunk(hashAddress)
	switch chunk.Type(c) {
	case chunk.HashLeafType:
		hm := dataToRefmap(c)
		for k, v := range hm {
			err := f(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	case chunk.HashNodeType:
		_, refs, _ := chunk.Parts(c)
		for _, ref := range refs {
			err := m.forEachHashEntry(ref, f)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return ErrNotHashChunk
	}

}

func (m *Modifier) deleteFromHash(addr uint64, key string) (uint64, error) {
	_, addr, err := m.deleteFromHashOnLevel(addr, 0, key)
	return addr, err
}

func (m *Modifier) deleteFromHashOnLevel(addr uint64, level int, key string) (bool, uint64, error) {
	c := m.Chunk(addr)
	t := chunk.Type(c)
	switch t {
	case chunk.HashLeafType:
		refmap := dataToRefmap(c)
		_, found := refmap[key]
		if !found {
			return false, addr, nil
		}
		delete(refmap, key)
		newAddr, err := m.Append(refmapToData(refmap))
		if err != nil {
			return false, 0, err
		}
		return true, newAddr, err
	case chunk.HashNodeType:
		_, refs, data := chunk.Parts(c)
		oldSize := binary.BigEndian.Uint64(data)

		idx := byteOfKey(key, level)

		deleted, newChildAddr, err := m.deleteFromHashOnLevel(refs[idx], level+1, key)
		if err != nil {
			return false, 0, err
		}

		newData := make([]byte, 8)
		if !deleted {
			return false, addr, nil
		}

		refs[idx] = newChildAddr

		oldSize--
		binary.BigEndian.PutUint64(newData, oldSize)

		newAddr, err := m.Append(chunk.Pack(chunk.HashNodeType, refs, newData))
		if err != nil {
			return false, 0, err
		}

		return true, newAddr, nil

	default:
		return false, 0, fmt.Errorf("deleteFromHash for chunk type %#v not implemented", t)
	}
}
