package array

import (
	"errors"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

var ErrNotArrayChunk = errors.New("Is not array chunk")

func CreateEmpty(s store.Store) (uint64, error) {
	la := &LazyArray{s: s, dirty: true}
	return la.Store()
}

func Prepend(s store.Store, addr uint64, vaddr uint64) (uint64, error) {
	la := &LazyArray{s: s, address: addr}
	err := la.Load()
	if err != nil {
		return 0, err
	}

	la.Prepend(vaddr)

	return la.Store()

}

// func NewLazyArray(s store.Store) *LazyArray {
//   return &LazyArray{
//     store: s
//   }
// }

type LazyArray struct {
	values   []uint64
	level    int
	count    uint64
	s        store.Store
	children []*LazyArray

	address uint64
	loaded  bool
	dirty   bool
}

func (la *LazyArray) load() {
	if la.loaded {
		return
	}
	err := la.Load()
	if err != nil {
		panic(err)
	}
}

func (la *LazyArray) Prepend(addr uint64) {
	la.load()
	la.values = append([]uint64{addr}, la.values...)
	la.count++
	la.dirty = true
}

func (la *LazyArray) Load() error {
	if la.loaded {
		return nil
	}

	if la.dirty {
		return errors.New("Can't load dirty instance")
	}

	c := la.s.Chunk(la.address)

	t, refs, data := chunk.Parts(c)

	if t != chunk.ArrayLeafType {
		return ErrNotArrayChunk
	}

	var l int
	var co uint64

	err := msgpack.Unmarshal(data, &l, &co)
	if err != nil {
		return err
	}

	la.level = l
	la.count = co

	la.loaded = true
	la.dirty = false

	if la.level == 0 {
		la.values = refs
		return nil
	}

	for _, r := range refs {
		la.children = append(la.children, &LazyArray{s: la.s, address: r})
	}
	return nil
}

func (la *LazyArray) Store() (uint64, error) {
	if !la.dirty {
		return la.address, nil
	}

	if la.level == 0 && len(la.children) > 0 {
		return 0, errors.New("Level 0 chunk can't have children!")
	}

	if la.level > 0 && len(la.values) > 0 {
		return 0, errors.New("Level >0 chunk can't have values!")
	}

	if la.level > 0 && len(la.children) == 0 {
		return 0, errors.New("Level >0 chunk must have children!")
	}

	data, err := msgpack.Marshal(la.level, la.count)

	if err != nil {
		return 0, err
	}

	refs := []uint64{}

	for _, c := range la.children {
		a, err := c.Store()
		if err != nil {
			return 0, err
		}
		refs = append(refs, a)
	}

	for _, v := range la.values {
		refs = append(refs, v)
	}

	ch := chunk.Pack(chunk.ArrayLeafType, refs, data)

	a, err := la.s.Append(ch)
	if err != nil {
		return 0, err
	}

	la.dirty = false
	la.address = a
	return a, nil
}
