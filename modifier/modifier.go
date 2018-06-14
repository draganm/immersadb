package modifier

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/modifier/ttfmap"
	"github.com/draganm/immersadb/store"
)

var ErrDoesNotExist = errors.New("Element does not exits")

type Modifier struct {
	store.Store
	chunkSize   int
	RootAddress uint64
	e           error
}

var _ DBWriter = &Modifier{}

func New(s store.Store, chunkSize int, rootAddress uint64) *Modifier {
	return &Modifier{
		Store:       s,
		chunkSize:   chunkSize,
		RootAddress: rootAddress,
	}
}

func (m *Modifier) Error() error {
	return m.e
}

func (m *Modifier) AbortIfError(err error) {
	// TODO gather errors
	if err != nil {
		m.e = err
	}
}

func (m *Modifier) ForEach(p dbpath.Path, f func(p dbpath.Path) bool) {
	if m.e != nil {
		return
	}

	exitErr := errors.New("exit")

	err := ttfmap.ForEach(m.Store, m.RootAddress, func(key string, ref uint64) error {
		cont := f(p.Append(key))
		if !cont {
			return exitErr
		}
		return nil
	})

	if err == exitErr {
		return
	}

	m.e = err
}

func (m *Modifier) ForEachAfter(p dbpath.Path, f func(p dbpath.Path) bool) {
	m.e = errors.New("ForEachAfter Not yet implemented")
}

func (m *Modifier) Read(p dbpath.Path, f func(r io.Reader) error) {
	if m.e != nil {
		return
	}

	addr, err := m.lookupAddress(p, m.RootAddress)
	if err != nil {
		m.e = err
		return
	}
	r := New(m.Store, m.chunkSize, addr).Data()

	m.e = f(r)
}

func (m *Modifier) TypeOf(p dbpath.Path) EntityType {
	if m.e != nil {
		return Unknown
	}
	addr, err := m.lookupAddress(p, m.RootAddress)
	if err != nil {
		return Unknown
	}
	return New(m.Store, m.chunkSize, addr).Type()
}

func (m *Modifier) CreateMap(path dbpath.Path) {
	if m.e != nil {
		return
	}

	last := path[len(path)-1]

	m.e = m.modify(path[:len(path)-1], func(vm *Modifier) error {

		switch last.(type) {
		case string:
			valueAddr, err := ttfmap.CreateEmpty(m.Store)
			if err != nil {
				return err
			}

			newRoot, err := ttfmap.Insert(m.Store, vm.RootAddress, last.(string), valueAddr)

			if err != nil {
				return err
			}

			vm.RootAddress = newRoot
			return nil
		case uint64:
			idx := last.(uint64)

			if idx != 0 {
				return errors.New("Can only append to the head of the array")
			}

			valueAddr, err := ttfmap.CreateEmpty(m.Store)
			if err != nil {
				return err
			}

			newRoot, err := vm.prependArray(vm.RootAddress, valueAddr)
			if err != nil {
				return err
			}

			vm.RootAddress = newRoot
			return nil
		default:
			return fmt.Errorf("Cannot create hash on %s: %#v is not supported as parent for Hash", path, last)
		}

	})

}

func (m *Modifier) CreateArray(path dbpath.Path) {
	if m.e != nil {
		return
	}

	last := path[len(path)-1]

	m.e = m.modify(path[:len(path)-1], func(vm *Modifier) error {
		switch last.(type) {
		case string:

			valueAddr, err := vm.createEmptyArrayLeaf()
			if err != nil {
				return err
			}

			newRoot, err := ttfmap.Insert(m.Store, vm.RootAddress, last.(string), valueAddr)

			if err != nil {
				return err
			}

			vm.RootAddress = newRoot
			return nil
		case uint64:
			valueAddr, err := vm.createEmptyArrayLeaf()
			if err != nil {
				return err
			}

			newRoot, err := vm.prependArray(vm.RootAddress, valueAddr)
			if err != nil {
				return err
			}

			vm.RootAddress = newRoot

			return nil

		default:
			panic(fmt.Sprintf("not yet implemented: %v", last))
		}
	})

}

func (m *Modifier) rootType() chunk.ChunkType {
	return chunk.Type(m.Chunk(m.RootAddress))
}

func (m *Modifier) modify(path dbpath.Path, f func(*Modifier) error) error {
	if len(path) == 0 {
		return f(m)
	}

	switch path[0].(type) {
	case string:
		key := path[0].(string)
		if m.rootType() != chunk.TTFMapNode {
			return errors.New("Value is not a hash")
		}

		address, err := ttfmap.Lookup(m.Store, m.RootAddress, key)
		if err != nil {
			return err
		}

		sub := New(m.Store, m.chunkSize, address)
		err = sub.modify(path[1:], f)
		if err != nil {
			return err
		}

		newRoot, err := ttfmap.Insert(m.Store, m.RootAddress, key, sub.RootAddress)
		if err != nil {
			return err
		}
		m.RootAddress = newRoot
		return nil
	case int:
		idx := path[0].(int)
		address, err := m.lookupArray(m.RootAddress, uint64(idx))
		if err != nil {
			return err
		}
		sub := New(m.Store, m.chunkSize, address)
		err = sub.modify(path[1:], f)
		if err != nil {
			return err
		}
		newRoot, err := m.setArrayValue(m.RootAddress, uint64(idx), sub.RootAddress)

		if err != nil {
			return err
		}

		m.RootAddress = newRoot
		return nil
	default:
		panic("not yet implemented")
	}

}

func (m *Modifier) CreateData(path dbpath.Path, f func(io.Writer) error) {

	if m.e != nil {
		return
	}

	last := path[len(path)-1]

	m.e = m.modify(path[:len(path)-1], func(vm *Modifier) error {
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

			newRoot, err := ttfmap.Insert(m.Store, vm.RootAddress, last.(string), valueAddr)
			if err != nil {
				return err
			}
			vm.RootAddress = newRoot
			return nil
		case uint64:
			idx := last.(uint64)
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

			newRootAddress, err := vm.prependArray(vm.RootAddress, valueAddr)

			if err != nil {
				return err
			}

			vm.RootAddress = newRootAddress

			return nil
		default:
			panic(fmt.Errorf("not yet implemented %#v", last))
		}

	})

}

func (m *Modifier) AddressOf(path dbpath.Path) uint64 {
	a, err := m.lookupAddress(path, m.RootAddress)
	if err != nil {
		m.e = err
		return 0
	}
	return a
}

func (m *Modifier) lookupAddress(path dbpath.Path, from uint64) (uint64, error) {

	for len(path) > 0 {
		switch path[0].(type) {
		case string:
			key := path[0].(string)
			if m.rootType() != chunk.TTFMapNode {
				return 0, errors.New("Value is not a hash")
			}

			address, err := ttfmap.Lookup(m.Store, from, key)
			if err != nil {
				return 0, err
			}

			from = address
		case uint64:
			var err error
			from, err = m.lookupArray(from, path[0].(uint64))
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

func (m *Modifier) Data() io.Reader {
	r, err := NewDataReader(m.Store, m.RootAddress)
	if err != nil {
		// TODO ErrorReader?
		panic(err)
	}
	return r
}

func (m *Modifier) Exists(path dbpath.Path) bool {
	_, err := m.lookupAddress(path, m.RootAddress)
	if err != nil {
		m.e = err
		return false
	}
	return true
}

var valueTypeByChunkType = map[chunk.ChunkType]EntityType{
	chunk.DataHeaderType: Data,
	chunk.TTFMapNode:     Map,
	chunk.ArrayLeafType:  Array,
	chunk.ArrayNodeType:  Array,
}

func (m *Modifier) Type() EntityType {
	addr := m.RootAddress
	chunkType := chunk.Type(m.Chunk(addr))
	t, found := valueTypeByChunkType[chunkType]
	if !found {
		return Unknown
	}
	return t
}

func (m *Modifier) Address() uint64 {
	return m.RootAddress
}

func (m *Modifier) HasPath(path dbpath.Path) bool {
	_, err := m.lookupAddress(path, m.RootAddress)
	if err != nil {
		return false
	}
	return true
}

func (m *Modifier) Size() uint64 {
	addr := m.RootAddress
	chunkType, _, data := chunk.Parts(m.Chunk(addr))
	switch chunkType {
	case chunk.TTFMapNode:
		// return uint64(len(refs))
		return 0
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

func (m *Modifier) clearMap(path dbpath.Path) error {
	return m.modify(path[:len(path)], func(mm *Modifier) error {
		rootAddres, err := ttfmap.CreateEmpty(m.Store)
		if err != nil {
			return err
		}
		mm.RootAddress = rootAddres
		return nil
	})
}

func (m *Modifier) Delete(path dbpath.Path) {
	if m.e != nil {
		return
	}
	lastElement := path[len(path)-1]
	m.e = m.modify(path[:len(path)-1], func(mm *Modifier) error {
		switch lastElement.(type) {
		case string:
			addr, err := ttfmap.Delete(m.Store, mm.RootAddress, lastElement.(string))
			if err != nil {
				return err
			}
			mm.RootAddress = addr
			return nil
		case uint64:
			addr, err := m.deleteFromArray(mm.RootAddress, lastElement.(uint64))
			if err != nil {
				return err
			}
			mm.RootAddress = addr
			return nil
		default:
			return fmt.Errorf("Delete not supported for type %#v", lastElement)
		}
	})

}
