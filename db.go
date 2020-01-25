package immersadb

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/draganm/immersadb/store"
	"github.com/draganm/immersadb/wbbtree"
	"github.com/pkg/errors"
)

type DB struct {
	root     store.Address
	st       store.Store
	txActive bool
	dir      string
	mu       sync.Mutex
}

//  Database file layout:
//  root - 8 bytes containing address of the root
//  layer-x - layers 1-3
//  transaction - layer 0

// default max sizes for layers:
// l1 - 10 megs
// l2 - 100 megs
// l3 - 1TB

func Open(path string) (*DB, error) {

	st := store.Store{nil}

	layerLimit := uint64(10 * 1024 * 1024)
	for i := 1; i < 4; i++ {
		layerFile := filepath.Join(path, fmt.Sprintf("layer-%d", i))
		lf, err := store.OpenOrCreateSegmentFile(layerFile, layerLimit)
		if err != nil {
			return nil, errors.Wrapf(err, "while opening layer file %s", layerFile)
		}
		st = append(st, lf)
		layerLimit *= 10
	}

	rootFileName := filepath.Join(path, "root")

	d, err := ioutil.ReadFile(rootFileName)
	if os.IsNotExist(err) {
		rootAddress, err := wbbtree.CreateEmpty(st[1:])
		if err != nil {
			return nil, errors.Wrap(err, "while creating empty root")
		}

		rootAddress = store.NewAddress(1, rootAddress.Position())

		d = make([]byte, 8)
		binary.BigEndian.PutUint64(d, uint64(rootAddress))
		err = ioutil.WriteFile(rootFileName, d, 0700)
		if err != nil {
			return nil, errors.Wrap(err, "while writing root address")
		}
	}

	if len(d) != 8 {
		return nil, errors.Errorf("root must have 8 bytes, has %d instead", len(d))
	}

	ra := binary.BigEndian.Uint64(d)

	rootAddr := store.Address(ra)

	return &DB{
		root: rootAddr,
		st:   st,
		dir:  path,
	}, nil
}

func (db *DB) ReadTransaction() *ReadTransaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	return &ReadTransaction{
		db.st,
		db.root,
	}
}

func (db *DB) Transaction() (*Transaction, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.txActive {
		// TODO add waiting or optimistic tx here
		return nil, errors.New("there is already a transaction in progress")
	}

	tx, err := newTransaction(db.st, db.root, db)
	if err != nil {
		return nil, errors.Wrap(err, "while creating transaction")
	}

	db.txActive = true

	return tx, nil
}

func (db *DB) commit(l0 *store.SegmentFile, newRoot store.Address) error {
	defer l0.CloseAndDelete()
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.txActive {
		return errors.New("cannot commit, no transaction was active")
	}

	db.txActive = false

	txStore := make(store.Store, len(db.st))
	copy(txStore, db.st)
	txStore[0] = l0

	newDBRoot, _, err := txStore.Commit(newRoot)
	if err != nil {
		return errors.Wrap(err, "while commiting transaction")
	}

	db.root = newDBRoot
	// TODO: write the root file!

	return nil

}
