package btree

import "github.com/draganm/immersadb/store"

func Get(s store.Store, root store.Address, key []byte) (store.Address, error) {

	n := &node{
		m:       15,
		address: root,
		store:   s,
	}

	return n.get(key)
}
