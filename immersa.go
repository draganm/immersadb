package immersadb

import (
	"reflect"
	"sync"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
)

// ImmersaDB represents an instance of the database.
type ImmersaDB struct {
	sync.RWMutex
	store     *store.FileStore
	chunkSize int
	listeners []*listenerState
}

// New creates a new instance of ImmersaDB.
func New(path string, chunkSize int) (*ImmersaDB, error) {

	s, err := store.NewFileStore(path)
	if err != nil {
		return nil, err
	}

	if s.LastChunkAddress() == 0 {
		s.Append(chunk.Pack(chunk.HashLeafType, nil, nil))
	}

	return &ImmersaDB{
		store:     s,
		chunkSize: chunkSize,
	}, nil
}

func (i *ImmersaDB) Transaction(t func(modifier.EntityWriter) error) error {
	i.Lock()
	defer i.Unlock()

	cs := store.NewCommitingStore(i.store)

	m := modifier.New(cs, i.chunkSize, cs.LastChunkAddress())
	err := t(m)
	if err != nil {
		return err
	}

	err = cs.Commit()
	if err != nil {
		return err
	}

	for _, l := range i.listeners {
		l.checkForChange(i)
	}
	return nil
}

func (i *ImmersaDB) ReadTransaction(t func(modifier.EntityReader) error) error {
	i.RLock()
	defer i.RUnlock()
	m := modifier.New(i.store, i.chunkSize, i.store.LastChunkAddress())
	return t(m)
}

func (i *ImmersaDB) readTransaction(t func(modifier.EntityReader) error) error {
	m := modifier.New(i.store, i.chunkSize, i.store.LastChunkAddress())
	return t(m)
}

type Listener interface {
	OnChange(r modifier.EntityReader)
}

type listenerState struct {
	matcher     modifier.DBPath
	listener    Listener
	latestState uint64
}

func (i *ImmersaDB) AddListener(matcher modifier.DBPath, f Listener) {
	i.Lock()
	defer i.Unlock()

	ls := &listenerState{
		matcher,
		f,
		0,
	}
	i.listeners = append(i.listeners, ls)
	ls.checkForChange(i)
}

type listenerFuncHolder func(r modifier.EntityReader)

func (l listenerFuncHolder) OnChange(r modifier.EntityReader) {
	l(r)
}

func (i *ImmersaDB) AddListenerFunc(matcher modifier.DBPath, f func(r modifier.EntityReader)) {
	i.AddListener(matcher, listenerFuncHolder(f))
}

func (i *ImmersaDB) RemoveListenerFunc(matcher modifier.DBPath, f func(r modifier.EntityReader)) {
	i.RemoveListener(matcher, listenerFuncHolder(f))
}

func (i *ImmersaDB) RemoveListener(matcher modifier.DBPath, f Listener) {
	i.Lock()
	defer i.Unlock()

	foundIdx := -1
	v1 := reflect.ValueOf(f)

	for idx, l := range i.listeners {
		v2 := reflect.ValueOf(l.listener)
		if reflect.DeepEqual(l.matcher, matcher) && v1.Pointer() == v2.Pointer() {
			foundIdx = idx
			break
		}
	}

	if foundIdx >= 0 {
		i.listeners = append(i.listeners[:foundIdx], i.listeners[foundIdx+1:]...)
	}
}

func (ls *listenerState) checkForChange(i *ImmersaDB) {
	i.readTransaction(func(r modifier.EntityReader) error {
		var err error
		re, err := r.EntityReaderFor(ls.matcher)
		if err != nil {
			return err
		}
		addr := re.Address()
		if addr != ls.latestState {
			sr, err := r.EntityReaderFor(ls.matcher)
			if err != nil {
				return err
			}
			ls.listener.OnChange(sr)
			ls.latestState = addr
		}
		return nil
	})

}
