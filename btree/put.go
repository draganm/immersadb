package btree

import "github.com/draganm/immersadb/store"

const M = 15

func Put(s store.Store, root store.Address, key []byte, value store.Address) (store.Address, error) {
	n := &node{
		m:       15,
		address: root,
		store:   s,
	}

	rn, err := insertIntoBtree(n, keyValue{key, value})
	if err != nil {
		return store.NilAddress, err
	}

	return rn.persist()

}

func CreateEmpty(s store.Store) (store.Address, error) {

	n := &node{
		Count:    0,
		Children: nil,
		KVS:      nil,
		address:  store.NilAddress,
		m:        M,
		store:    s,
	}

	return n.persist()
}
