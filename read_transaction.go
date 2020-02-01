package immersadb

import (
	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/store"
	"github.com/draganm/immersadb/wbbtree"
	"github.com/pkg/errors"
	"io/ioutil"
)

type ReadTransaction struct {
	st   store.Store
	root store.Address
}

func (t *ReadTransaction) Count(path string) (uint64, error) {
	pa, err := t.pathElementAddress(path)
	if err != nil {
		return 0, err
	}
	return wbbtree.Count(t.st, pa)
}

func (t *ReadTransaction) pathElementAddress(path string) (store.Address, error) {
	parts, err := dbpath.Split(path)
	if err != nil {
		return store.NilAddress, err
	}

	ad := t.root

	for _, p := range parts[:len(parts)] {
		ad, err = wbbtree.Search(t.st, ad, []byte(p))
		if err != nil {
			return store.NilAddress, err
		}
	}

	return ad, nil
}

func (t *ReadTransaction) Get(path string) ([]byte, error) {
	pa, err := t.pathElementAddress(path)
	if err != nil {
		return nil, err
	}
	r, err := data.NewReader(pa, t.st)
	if err != nil {
		return nil, errors.Wrap(err, "while creating reader")
	}

	return ioutil.ReadAll(r)

}
