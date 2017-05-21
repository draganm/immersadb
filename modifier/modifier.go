package modifier

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
)

var ErrDoesNotExist = errors.New("Element does not exits")

type Modifier struct {
	store.Store
	chunkSize   int
	rootAddress uint64
}

func New(s store.Store, chunkSize int, rootAddress uint64) *Modifier {

	return &Modifier{
		Store:       s,
		chunkSize:   chunkSize,
		rootAddress: rootAddress,
	}
}

func (m *Modifier) rootType() chunk.ChunkType {
	return chunk.Type(m.Chunk(m.rootAddress))
}

func (m *Modifier) modify(path DBPath, f func(*Modifier) error) error {
	if len(path) == 0 {
		return f(m)
	}

	switch path[0].(type) {
	case string:
		key := path[0].(string)
		if m.rootType() != chunk.HashLeafType && m.rootType() != chunk.HashNodeType {
			return errors.New("Value is not a hash")
		}

		address, err := m.lookupAddressInHash(m.rootAddress, key)
		if err != nil {
			return err
		}

		sub := New(m.Store, m.chunkSize, address)
		err = sub.modify(path[1:], f)
		if err != nil {
			return err
		}
		newRoot, err := m.addToHash(m.rootAddress, key, sub.rootAddress)
		if err != nil {
			return err
		}
		m.rootAddress = newRoot
		return nil
	case int:
		idx := path[0].(int)
		address, err := m.lookupArray(m.rootAddress, uint64(idx))
		if err != nil {
			return err
		}
		sub := New(m.Store, m.chunkSize, address)
		err = sub.modify(path[1:], f)
		if err != nil {
			return err
		}
		newRoot, err := m.setArrayValue(m.rootAddress, uint64(idx), sub.rootAddress)

		if err != nil {
			return err
		}

		m.rootAddress = newRoot
		return nil
	default:
		panic("not yet implemented")
	}

}

func (m *Modifier) CreateHash(path DBPath) error {

	last := path[len(path)-1]

	return m.modify(path[:len(path)-1], func(vm *Modifier) error {

		switch last.(type) {
		case string:
			valueAddr, err := vm.Append(refmapToData(map[string]uint64{}))
			if err != nil {
				return err
			}

			newRoot, err := vm.addToHash(vm.rootAddress, last.(string), valueAddr)
			if err != nil {
				return err
			}

			vm.rootAddress = newRoot
			return nil
		case int:
			idx := last.(int)

			if idx != 0 {
				return errors.New("Can only append to the head of the array")
			}

			valueAddr, err := vm.Append(refmapToData(map[string]uint64{}))
			if err != nil {
				return err
			}

			newRoot, err := vm.prependArray(vm.rootAddress, valueAddr)
			if err != nil {
				return err
			}

			vm.rootAddress = newRoot
			return nil
		default:
			return fmt.Errorf("Cannot create hash on %s: %#v is not supported as parent for Hash", path, last)
		}

	})

}

func (m *Modifier) CreateArray(path DBPath) error {

	last := path[len(path)-1]

	return m.modify(path[:len(path)-1], func(vm *Modifier) error {
		switch last.(type) {
		case string:

			valueAddr, err := vm.createEmptyArrayLeaf()
			if err != nil {
				return err
			}

			newRoot, err := vm.addToHash(vm.rootAddress, last.(string), valueAddr)
			if err != nil {
				return err
			}

			vm.rootAddress = newRoot
			return nil
		default:
			panic("not yet implemented")
		}
	})

}

func (m *Modifier) CreateData(path DBPath, f func(io.Writer) error) error {

	last := path[len(path)-1]

	return m.modify(path[:len(path)-1], func(vm *Modifier) error {
		switch last.(type) {
		case string:

			w := NewDataWriter(vm.Store, vm.chunkSize)
			err := f(w)
			if err != nil {
				return err
			}

			valueAddr, err := w.Close()
			if err != nil {
				return err
			}

			newRoot, err := vm.addToHash(vm.rootAddress, last.(string), valueAddr)
			if err != nil {
				return err
			}
			vm.rootAddress = newRoot
			return nil
		case int:
			idx := last.(int)
			if idx != 0 {
				return errors.New("Only append to array head is supported")
			}
			w := NewDataWriter(vm.Store, vm.chunkSize)
			err := f(w)
			if err != nil {
				return err
			}

			valueAddr, err := w.Close()
			if err != nil {
				return err
			}

			newRootAddress, err := vm.prependArray(vm.rootAddress, valueAddr)

			if err != nil {
				return err
			}

			vm.rootAddress = newRootAddress

			return nil
		default:
			panic("not yet implemented")
		}

	})

}

func (m *Modifier) lookupAddress(path DBPath, from uint64) (uint64, error) {

	for len(path) > 0 {
		switch path[0].(type) {
		case string:
			key := path[0].(string)
			if m.rootType() != chunk.HashLeafType && m.rootType() != chunk.HashNodeType {
				return 0, errors.New("Value is not a hash")
			}

			address, err := m.lookupAddressInHash(from, key)
			if err != nil {
				return 0, err
			}

			from = address
		case int:
			var err error
			from, err = m.lookupArray(from, uint64(path[0].(int)))
			if err != nil {
				return 0, err
			}
		default:
			log.Panicf("lookupAddress for %#v: not yet implemented", path[0])
		}
		path = path[1:]
	}
	return from, nil

}

func (m *Modifier) Data() (io.Reader, error) {
	return NewDataReader(m.Store, m.rootAddress)
}

func (m *Modifier) ForEachHashEntry(f func(key string, reader EntityReader) error) error {
	return m.forEachHashEntry(m.rootAddress, func(key string, ref uint64) error {
		return f(key, New(m.Store, m.chunkSize, ref))
	})
}

func (m *Modifier) ForEachArrayElement(f func(index uint64, reader EntityReader) error) error {
	return m.forEachArrayElementStartingWithIndex(0, m.rootAddress, func(idx, valueAddr uint64) error {
		return f(idx, New(m.Store, m.chunkSize, valueAddr))
	})
}

func (m *Modifier) Exists(path DBPath) bool {
	_, err := m.lookupAddress(path, m.rootAddress)
	if err != nil {
		return false
	}
	return true
}

var valueTypeByChunkType = map[chunk.ChunkType]EntityType{
	chunk.DataHeaderType: Data,
	chunk.HashLeafType:   Hash,
	chunk.HashNodeType:   Hash,
	chunk.ArrayLeafType:  Array,
	chunk.ArrayNodeType:  Array,
}

func (m *Modifier) Type() EntityType {
	addr := m.rootAddress
	chunkType := chunk.Type(m.Chunk(addr))
	t, found := valueTypeByChunkType[chunkType]
	if !found {
		return Unknown
	}
	return t
}

func (m *Modifier) Address() uint64 {
	return m.rootAddress
}

func (m *Modifier) HasPath(path DBPath) bool {
	_, err := m.lookupAddress(path, m.rootAddress)
	if err != nil {
		return false
	}
	return true
}

func (m *Modifier) Size() uint64 {
	addr := m.rootAddress
	chunkType, refs, data := chunk.Parts(m.Chunk(addr))
	switch chunkType {
	case chunk.HashLeafType:
		return uint64(len(refs))
	case chunk.HashNodeType:
		return binary.BigEndian.Uint64(data)
	case chunk.ArrayNodeType, chunk.ArrayLeafType:
		// switching for the type, so error should
		// never be returned
		s, _ := m.arraySize(addr)
		return s
	case chunk.DataHeaderType:
		return binary.BigEndian.Uint64(data)
	}
	return 0
}

func (m *Modifier) EntityReaderFor(path DBPath) (EntityReader, error) {
	addr, err := m.lookupAddress(path, m.rootAddress)
	if err != nil {
		return nil, err
	}
	return New(m.Store, m.chunkSize, addr), nil
}

func (m *Modifier) Delete(path DBPath) error {
	lastElement := path[len(path)-1]
	return m.modify(path[:len(path)-1], func(mm *Modifier) error {
		switch lastElement.(type) {
		case string:
			addr, err := mm.deleteFromHash(mm.rootAddress, lastElement.(string))
			if err != nil {
				return err
			}
			mm.rootAddress = addr
			return nil
		case int:
			addr, err := m.deleteFromArray(mm.rootAddress, uint64(lastElement.(int)))
			if err != nil {
				return err
			}
			mm.rootAddress = addr
			return nil
		default:
			return fmt.Errorf("Delete not supported for type %#v", lastElement)
		}
	})

}
