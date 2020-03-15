package immersadb

import (
	"io/ioutil"

	"github.com/draganm/immersadb/btree"
	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
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
	return btree.Count(t.st, pa)
}

func (t *ReadTransaction) pathElementAddress(path string) (store.Address, error) {
	parts, err := dbpath.Split(path)
	if err != nil {
		return store.NilAddress, err
	}

	ad := t.root

	for _, p := range parts[:len(parts)] {
		ad, err = btree.Get(t.st, ad, []byte(p))
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

func (t *ReadTransaction) Exists(path string) (bool, error) {

	_, err := t.pathElementAddress(path)
	if errors.Cause(err) == btree.ErrNotFound {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (t *ReadTransaction) Discard() {
	t.st.FinishUse()
}
