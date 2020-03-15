package btree

import "github.com/draganm/immersadb/store"

func Count(s store.Store, root store.Address) (uint64, error) {
	n := &node{
		address: root,
		store:   s,
	}

	err := n.load()
	if err != nil {
		return 0, err
	}

	return n.Count, nil
}
