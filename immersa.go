package immersadb

import (
	"reflect"
	"sync"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
)

const chunkSize = 32 * 1024

// ImmersaDB represents an instance of the database.
type ImmersaDB struct {
	sync.RWMutex
	store       *store.SegmentedStore
	segmentSize int
	listeners   []*listenerState
}

// New creates a new instance of ImmersaDB.
func New(path string, segmentSize int) (*ImmersaDB, error) {

	s, err := store.NewSegmentedStore(path, segmentSize)
	if err != nil {
		return nil, err
	}

	if s.NextChunkAddress() == 0 {
		addr, err := s.Append(chunk.Pack(chunk.HashLeafType, nil, nil))
		if err != nil {
			return nil, err
		}

		_, err = s.Append(chunk.NewCommitChunk(addr))
		if err != nil {
			return nil, err
		}

	}

	return &ImmersaDB{
		store:       s,
		segmentSize: segmentSize,
	}, nil
}

func (i *ImmersaDB) Transaction(t func(modifier.EntityWriter) error) error {
	i.Lock()
	defer i.Unlock()

	cs := store.NewCommitingStore(i.store)

	_, refs, _ := chunk.Parts(cs.Chunk(cs.NextChunkAddress() - chunk.CommitChunkSize))

	m := modifier.New(cs, chunkSize, refs[0])
	err := t(m)
	if err != nil {
		return err
	}

	cs.Append(chunk.NewCommitChunk(m.RootAddress))

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
	return i.readTransaction(t)
}

func (i *ImmersaDB) readTransaction(t func(modifier.EntityReader) error) error {
	_, refs, _ := chunk.Parts(i.store.Chunk(i.store.NextChunkAddress() - chunk.CommitChunkSize))
	m := modifier.New(i.store, chunkSize, refs[0])
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
