package immersadb

import (
	"path/filepath"

	serrors "errors"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/store"
	"github.com/draganm/immersadb/wbbtree"
	"github.com/pkg/errors"
)

type Transaction struct {
	st   store.Store
	root store.Address
}

func newTransaction(st store.Store, root store.Address, dir string) (*Transaction, error) {
	txFilePath := filepath.Join(dir, "transaction")
	// TODO what's a meaningful size of the tx segment file?
	// auto-extending anonymous mmap?

	l0, err := store.OpenOrCreateSegmentFile(txFilePath, 10*1024*1024)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening segment file %s", txFilePath)
	}

	txStore := make(store.Store, len(st))
	copy(txStore, st)
	txStore[0] = l0

	return &Transaction{
		st:   txStore,
		root: root,
	}, nil

}

var ErrAlreadyExists = serrors.New("Already exists")

func (t *Transaction) CreateMap(path string) error {
	return t.modifyPath(path, func(ad store.Address, key string) (store.Address, error) {
		_, err := wbbtree.Search(t.st, ad, []byte(key))
		if err == nil {
			return store.NilAddress, ErrAlreadyExists
		}
		if errors.Cause(err) != wbbtree.ErrNotFound {
			return store.NilAddress, err
		}

		ea, err := wbbtree.CreateEmpty(t.st)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating empty map")
		}

		return wbbtree.Insert(t.st, ad, []byte(key), ea)
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
		ca, err := wbbtree.Search(st, ad, []byte(path[0]))
		if err != nil {
			return store.NilAddress, err
		}
		nca, err := modifyPath(st, ca, path[1:], f)
		if err != nil {
			return store.NilAddress, err
		}
		return wbbtree.Insert(st, ad, []byte(path[0]), nca)
	}

	return f(ad, path[0])

}

func (t *Transaction) Count(path string) (uint64, error) {
	pa, err := t.pathElementAddress(path)
	if err != nil {
		return 0, err
	}
	return wbbtree.Count(t.st, pa)
}

func (t *Transaction) pathElementAddress(path string) (store.Address, error) {
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
