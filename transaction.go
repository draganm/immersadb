package immersadb

import (
	serrors "errors"

	"github.com/draganm/immersadb/btree"
	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type Transaction struct {
	*ReadTransaction
	db *DB
}

func newTransaction(st store.Store, root store.Address, db *DB) (*Transaction, error) {

	txStore, err := st.WithTransaction()
	if err != nil {
		return nil, errors.Wrap(err, "while opening tx file")
	}

	txStore.StartUse()

	return &Transaction{
		ReadTransaction: &ReadTransaction{
			st:   txStore,
			root: root,
		},
		db: db,
	}, nil

}

var ErrAlreadyExists = serrors.New("Already exists")

func (t *Transaction) CreateMap(path string) error {
	return t.modifyPath(path, func(ad store.Address, key string) (store.Address, error) {
		_, err := btree.Get(t.st, ad, []byte(key))
		if err == nil {
			return store.NilAddress, ErrAlreadyExists
		}
		if errors.Cause(err) != btree.ErrNotFound {
			return store.NilAddress, err
		}

		ea, err := btree.CreateEmpty(t.st)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating empty map")
		}

		return btree.Put(t.st, ad, []byte(key), ea)
	})
}

func (t *Transaction) modifyPath(path string, f func(ad store.Address, key string) (store.Address, error)) error {
	pth, err := dbpath.Split(path)
	if err != nil {
		return errors.Wrapf(err, "while parsing dbpath %q", path)
	}
	nr, err := modifyPath(t.st, t.root, pth, f)
	if err != nil {
		return errors.Wrap(err, "while modifying path")
	}
	t.root = nr
	return nil
}

func modifyPath(st store.Store, ad store.Address, path []string, f func(ad store.Address, key string) (store.Address, error)) (store.Address, error) {

	if len(path) == 0 {
		return store.NilAddress, errors.New("attempted to modify parent of root")
	}

	if len(path) > 1 {
		ca, err := btree.Get(st, ad, []byte(path[0]))
		if err != nil {
			return store.NilAddress, err
		}
		nca, err := modifyPath(st, ca, path[1:], f)
		if err != nil {
			return store.NilAddress, err
		}
		return btree.Put(st, ad, []byte(path[0]), nca)
	}

	return f(ad, path[0])

}

func (t *Transaction) Commit() error {
	return t.db.commit(t.st[0], t.root)
}

func (t *Transaction) Rollback() error {
	return t.db.rollback(t.st[0])
}

func (t *Transaction) Put(path string, d []byte) error {
	return t.modifyPath(path, func(ad store.Address, key string) (store.Address, error) {
		da, err := data.StoreData(t.st, d, t.db.dataSegmentSize, t.db.dataFanout)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while storing data")
		}
		ra, err := btree.Put(t.st, ad, []byte(key), da)
		if err != nil {
			return store.NilAddress, errors.Wrapf(err, "while inserting %q into %s", key, ad)
		}
		return ra, nil
	})
}
