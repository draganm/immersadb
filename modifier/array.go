package modifier

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/draganm/immersadb/chunk"
)

var ErrNotArrayChunk = errors.New("Is not array chunk")
var ErrNotFound = errors.New("Not found")

const maxArrayDegree = 4

func (m *Modifier) createEmptyArrayLeaf() (uint64, error) {
	return m.Append(chunk.Pack(chunk.ArrayLeafType, nil, nil))
}

func (m *Modifier) arraySize(addr uint64) (uint64, error) {
	t, refs, data := chunk.Parts(m.Chunk(addr))
	switch t {
	case chunk.ArrayLeafType:
		return uint64(len(refs)), nil
	case chunk.ArrayNodeType:
		sizes := make([]uint64, len(refs))
		for i := range refs {
			sizes[i] = binary.BigEndian.Uint64(data[i*8+2:])
		}
		sum := uint64(0)
		for _, s := range sizes {
			sum += s
		}
		return sum, nil
	default:
		return 0, ErrNotArrayChunk
	}

}

func (m *Modifier) lookupArray(addr, idx uint64) (uint64, error) {
	t, refs, data := chunk.Parts(m.Chunk(addr))
	switch t {
	case chunk.ArrayLeafType:
		if idx >= uint64(len(refs)) {
			return 0, ErrNotFound
		}
		return refs[int(idx)], nil
	case chunk.ArrayNodeType:
		sizes := make([]uint64, len(refs))
		for i := range refs {
			sizes[i] = binary.BigEndian.Uint64(data[i*8+2:])
		}
		for i, s := range sizes {
			if idx < s {
				return m.lookupArray(refs[i], idx)
			}
			idx -= s
		}
		return 0, ErrNotFound
	default:
		return 0, ErrNotArrayChunk
	}
}

func (m *Modifier) prependLeafArray(arrayAddr, valueAddr uint64) (uint64, error) {
	_, refs, _ := chunk.Parts(m.Chunk(arrayAddr))
	if len(refs) < maxArrayDegree {
		// refs = append(refs, valueAddr)
		newRefs := make([]uint64, len(refs)+1)
		copy(newRefs[1:], refs)
		newRefs[0] = valueAddr
		return m.Append(chunk.Pack(chunk.ArrayLeafType, newRefs, nil))
	}
	refs = make([]uint64, maxArrayDegree)
	sizes := make([]uint64, maxArrayDegree)
	refs[maxArrayDegree-1] = arrayAddr
	sizes[maxArrayDegree-1] = maxArrayDegree

	oneElementLeafAddr, err := m.Append(chunk.Pack(chunk.ArrayLeafType, []uint64{valueAddr}, nil))
	if err != nil {
		return 0, err
	}

	refs[maxArrayDegree-2] = oneElementLeafAddr
	sizes[maxArrayDegree-2] = 1

	emptyLeafAddr, nil := m.Append(chunk.Pack(chunk.ArrayLeafType, nil, nil))
	if err != nil {
		return 0, err
	}

	for i := 0; i < maxArrayDegree-2; i++ {
		refs[i] = emptyLeafAddr
	}

	data := make([]byte, maxArrayDegree*8+2)
	for i, s := range sizes {
		binary.BigEndian.PutUint64(data[i*8+2:], s)
	}

	binary.BigEndian.PutUint16(data, uint16(1))
	return m.Append(chunk.Pack(chunk.ArrayNodeType, refs, data))

}

func pow(x uint64, y uint16) uint64 {
	res := x
	for i := uint16(1); i < y; i++ {
		res *= x
	}
	return res
}

func (m *Modifier) forEachArrayElement(arrayAddr uint64, f func(idx, valueAddr uint64) error) error {
	return m.forEachArrayElementStartingWithIndex(uint64(0), arrayAddr, f)
}

func (m *Modifier) forEachArrayElementStartingWithIndex(idx, arrayAddr uint64, f func(idx, valueAddr uint64) error) error {
	t, refs, data := chunk.Parts(m.Chunk(arrayAddr))
	switch t {
	case chunk.ArrayLeafType:
		for i, r := range refs {
			err := f(idx+uint64(i), r)
			if err != nil {
				return err
			}
		}
	case chunk.ArrayNodeType:
		sizes := make([]uint64, len(refs))
		for i := range refs {
			sizes[i] = binary.BigEndian.Uint64(data[2+8*i:])
		}
		for i, s := range sizes {
			err := m.forEachArrayElementStartingWithIndex(idx, refs[i], f)
			if err != nil {
				return err
			}
			idx += s
		}
	default:
		return ErrNotArrayChunk
	}
	return nil
}

func (m *Modifier) setArrayValue(arrayAddr, index, valueAddr uint64) (uint64, error) {
	t, refs, data := chunk.Parts(m.Chunk(arrayAddr))
	switch t {
	case chunk.ArrayLeafType:
		if uint64(len(refs)) < index {
			return 0, ErrDoesNotExist
		}
		refs[int(index)] = valueAddr
		return m.Append(chunk.Pack(chunk.ArrayLeafType, refs, data))
	case chunk.ArrayNodeType:
		for i := range refs {
			size := binary.BigEndian.Uint64(data[i*8+2:])

			if size < index {
				newChildAddr, err := m.setArrayValue(refs[i], index, valueAddr)
				if err != nil {
					return 0, err
				}
				refs[i] = newChildAddr
				return m.Append(chunk.Pack(chunk.ArrayNodeType, refs, data))
			}
			index -= size
		}
		return 0, ErrNotFound

	default:
		return 0, ErrNotArrayChunk
	}

}

func (m *Modifier) prependArray(arrayAddr, valueAddr uint64) (uint64, error) {
	t, refs, data := chunk.Parts(m.Chunk(arrayAddr))
	switch t {
	case chunk.ArrayLeafType:
		return m.prependLeafArray(arrayAddr, valueAddr)
	case chunk.ArrayNodeType:
		level := binary.BigEndian.Uint16(data)
		maxForLowerLevel := pow(maxArrayDegree, level-1)
		sizes := make([]uint64, len(refs))
		for i := range refs {
			sizes[i] = binary.BigEndian.Uint64(data[i*8+2:])
		}

		bucketIndex := 0

		for i, s := range sizes {
			if s > 0 {
				bucketIndex = i
				break
			}
		}

		if sizes[bucketIndex] >= maxForLowerLevel && bucketIndex > 0 {
			bucketIndex--
		}

		if sizes[bucketIndex] == maxForLowerLevel {

			totalSize := uint64(0)
			for _, s := range sizes {
				totalSize += s
			}

			refs = make([]uint64, maxArrayDegree)
			sizes = make([]uint64, maxArrayDegree)
			refs[maxArrayDegree-1] = arrayAddr
			sizes[maxArrayDegree-1] = totalSize

			oneElementLeafAddr, err := m.Append(chunk.Pack(chunk.ArrayLeafType, []uint64{valueAddr}, nil))
			if err != nil {
				return 0, err
			}

			refs[maxArrayDegree-2] = oneElementLeafAddr
			sizes[maxArrayDegree-2] = 1

			emptyLeafAddr, nil := m.Append(chunk.Pack(chunk.ArrayLeafType, nil, nil))
			if err != nil {
				return 0, err
			}

			for i := 0; i < maxArrayDegree-2; i++ {
				refs[i] = emptyLeafAddr
			}

			data = make([]byte, maxArrayDegree*8+2)
			for i, s := range sizes {
				binary.BigEndian.PutUint64(data[i*8+2:], s)
			}

			binary.BigEndian.PutUint16(data, uint16(level+1))
			return m.Append(chunk.Pack(chunk.ArrayNodeType, refs, data))

		}
		changedAddr, err := m.prependArray(refs[bucketIndex], valueAddr)
		if err != nil {
			return 0, err
		}

		refs[bucketIndex] = changedAddr
		sizes[bucketIndex]++

		data2 := make([]byte, len(data))
		copy(data2, data)
		data = data2

		for i, s := range sizes {
			binary.BigEndian.PutUint64(data[i*8+2:], s)
		}

		return m.Append(chunk.Pack(chunk.ArrayNodeType, refs, data))

	default:
		return 0, ErrNotArrayChunk
	}
}

func (m *Modifier) deleteFromArray(arrayAddr, index uint64) (uint64, error) {
	t, refs, data := chunk.Parts(m.Chunk(arrayAddr))

	switch t {
	case chunk.ArrayLeafType:
		if index >= uint64(len(refs)) {
			return 0, ErrNotFound
		}
		iindex := int(index)
		refs = append(refs[:iindex], refs[iindex+1:]...)
		return m.Append(chunk.Pack(chunk.ArrayLeafType, refs, nil))
	case chunk.ArrayNodeType:

		sizes := make([]uint64, len(refs))
		childIndex := -1
		level := binary.BigEndian.Uint16(data)
		for i := range refs {
			size := binary.BigEndian.Uint64(data[i*8+2:])
			sizes[i] = size
		}

		for i, s := range sizes {
			if index < s {
				childIndex = i
				break
			}
			index -= s
		}

		if childIndex < 0 {
			return 0, ErrNotFound
		}

		newChildRef, err := m.deleteFromArray(refs[childIndex], index)
		if err != nil {
			return 0, err
		}

		refs[childIndex] = newChildRef

		sizes[childIndex]--
		if sizes[childIndex] == 0 {
			refs = append(refs[:childIndex], refs[childIndex+1:]...)
			sizes = append(sizes[:childIndex], sizes[childIndex+1:]...)
			// newChildRef points to an empty array, use this!
			refs = append([]uint64{newChildRef}, refs...)
			sizes = append([]uint64{0}, sizes...)
		}

		newData := make([]byte, 2+len(refs)*8)
		binary.BigEndian.PutUint16(newData, level)
		for i, s := range sizes {
			binary.BigEndian.PutUint64(newData[2+i*8:], s)
		}

		return m.Append(chunk.Pack(chunk.ArrayNodeType, refs, newData))

	default:

		return 0, fmt.Errorf("Deleting Array from chunk type %#v not yet implemented", t)
	}
}
