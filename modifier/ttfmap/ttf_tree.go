package ttfmap

import (
	"github.com/draganm/immersadb/store"
)

func CreateEmpty(s store.Store) (uint64, error) {
	return (&LazyNode{
		store:  s,
		loaded: true,
		dirty:  true,
	}).Store()
}

func Insert(s store.Store, addr uint64, key string, valAddr uint64) (uint64, error) {
	root := NewLazyNode(s, addr)
	newRoot := root.Insert(key, valAddr)
	if newRoot != nil {
		return newRoot.Store()
	}
	return root.Store()
}

func Validate(s store.Store, addr uint64) error {
	root := NewLazyNode(s, addr)
	return root.Validate()
}

func Lookup(s store.Store, addr uint64, key string) (uint64, error) {
	root := NewLazyNode(s, addr)
	return root.Lookup(key)
}

func Delete(s store.Store, addr uint64, key string) (uint64, error) {
	root := NewLazyNode(s, addr)
	err := root.Delete(key)
	if err != nil {
		return 0, err
	}
	return root.Store()
}

func ForEach(s store.Store, addr uint64, f func(key string, value uint64) error) error {
	root := NewLazyNode(s, addr)
	return root.ForEach(f)
}
