package immersadb

import (
	"sync"

	"github.com/draganm/immersadb/store"
	"github.com/draganm/immersadb/wbbtree"
	"github.com/pkg/errors"
)

type DB struct {
	dataSegmentSize int
	dataFanout      int
	root            store.Address
	st              store.Store
	txActive        bool
	dir             string
	mu              sync.Mutex
}

//  Database file layout:
//  root - 8 bytes containing address of the root
//  lx-id - layers 1-3
//  transaction-id - layer 0

func Open(path string) (*DB, error) {

	st, err := store.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "while opening store")
	}

	var root store.Address
	if st.IsEmpty() {
		_, err = wbbtree.CreateEmpty(st[1:])
		if err != nil {
			return nil, errors.Wrap(err, "while creating empty root")
		}
	}

	root = st.Root()

	return &DB{
		root:            root,
		st:              st,
		dir:             path,
		dataSegmentSize: 256 * 1024,
		dataFanout:      16,
	}, nil
}

func (db *DB) NewReadTransaction() *ReadTransaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	return &ReadTransaction{
		db.st,
		db.root,
	}
}

func (db *DB) NewTransaction() (*Transaction, error) {
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

	newDBRoot, ns, err := txStore.Commit(newRoot)
	if err != nil {
		return errors.Wrap(err, "while commiting transaction")
	}
	// TODO close the old store diff
	// fmt.Println("new root", newDBRoot)
	db.root = newDBRoot
	db.st = ns

	return nil

}

func (db *DB) rollback(l0 *store.SegmentFile) error {

	defer l0.CloseAndDelete()
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.txActive {
		return errors.New("cannot rollback, no transaction was active")
	}

	db.txActive = false

	return nil

}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.st.Close()
}
