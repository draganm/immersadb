package immersadb

import (
	"fmt"
	"sync"

	"github.com/draganm/immersadb/btree"
	"github.com/draganm/immersadb/store"
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
		_, err = btree.CreateEmpty(st[1:])
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

	db.st.StartUse()

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

	defer func() {
		go l0.CloseAndDelete()
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.txActive {
		return errors.New("cannot commit, no transaction was active")
	}

	db.txActive = false

	if newRoot == db.root {
		return nil
	}

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

	for i := range ns {
		if db.st[i] != ns[i] {
			go db.st[i].CloseAndDelete()
		}
	}

	txStore.FinishUse()
	ns.FinishUse()

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

func (db *DB) Transaction(f func(tx *Transaction) error) error {
	tx, err := db.NewTransaction()
	if err != nil {
		return errors.Wrap(err, "while creating transaction")
	}

	err = f(tx)
	if err != nil {
		rbErr := tx.Rollback()
		if err != nil {
			return errors.Wrap(rbErr, "while rolling back transaction")
		}
		return err
	}

	return tx.Commit()
}

func (db *DB) PrintStats() {
	db.mu.Lock()
	defer db.mu.Unlock()

	rs := db.st.GetSegment(db.root)

	fmt.Println("total data size", rs.GetTotalTreeSize(), "bytes")
	for i := 1; i < 4; i++ {
		ub := db.st[i].UsedBytes()
		lts := rs.GetLayerTotalSize(i)
		garbagePercent := 0.0
		if ub > 0 {
			garbagePercent = 100.0 * float64(ub-lts) / float64(ub)
		}
		fmt.Printf("Layer %d: size %d, used %d, garbage %d bytes (%0.2f%%)\n", i, ub, lts, ub-lts, garbagePercent)

	}
}
