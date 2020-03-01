package trie

import "github.com/draganm/immersadb/store"

func CreateEmpty(s store.Store) (store.Address, error) {
	t := newEmptyTrie(s)
	return t.persist()
}

func Put(s store.Store, a store.Address, key []byte, value store.Address) (store.Address, error) {
	tr := newPersistedTrie(s, a)

	changed, err := tr.insert(key, value)
	if err != nil {
		return store.NilAddress, err
	}

	if !changed {
		return a, nil
	}

	return tr.persist()

}

func Get(s store.Store, a store.Address, key []byte) (store.Address, error) {
	tr := newPersistedTrie(s, a)
	return tr.get(key)
}
