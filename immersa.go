package immersadb

import (
	"errors"
	"log"
	"reflect"
	"sync"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/graph"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/modifier/ttfmap"
	"github.com/draganm/immersadb/store"
)

const chunkSize = 32 * 1024

var ErrCouldNotRecover = errors.New("Could not recover: Can't find any commit chunks!")

// ImmersaDB represents an instance of the database.
type ImmersaDB struct {
	sync.RWMutex
	store     *store.GCStore
	listeners []*listenerState
}

// New creates a new instance of ImmersaDB.
func New(path string) (*ImmersaDB, error) {

	s, err := store.NewGCStore(path)
	if err != nil {
		log.Println(err)

		return nil, err
	}

	if s.NextChunkAddress() == 0 {
		addr, err := ttfmap.CreateEmpty(s)
		if err != nil {
			return nil, err
		}

		_, err = s.Append(chunk.NewCommitChunk(addr))
		if err != nil {
			return nil, err
		}

	}

	commitAddress := s.NextChunkAddress() - chunk.CommitChunkSize

	invalidCommitChunk := s.Chunk(commitAddress) == nil

	if invalidCommitChunk {
		lastSeenCommit := s.NextChunkAddress()

		for addr := s.FirstChunkAddress(); addr < s.NextChunkAddress(); {
			c := s.Chunk(addr)
			t := chunk.Type(c)
			if t == chunk.CommitType {
				lastSeenCommit = addr
			}
			addr += uint64(len(c) + 2)
		}

		if lastSeenCommit == s.NextChunkAddress() {
			return nil, ErrCouldNotRecover
		}

		c := s.Chunk(lastSeenCommit)

		_, err = s.Append(c)

		if err != nil {
			return nil, err
		}

	}

	return &ImmersaDB{
		store: s,
	}, nil
}

func (i *ImmersaDB) DumpGraph() {
	graph.DumpGraph(i.store, i.store.NextChunkAddress()-chunk.CommitChunkSize)
}

func (i *ImmersaDB) Close() error {
	i.Lock()
	defer i.Unlock()
	return i.store.Close()
}

func (i *ImmersaDB) Transaction(t func(m modifier.DBWriter) error) error {
	i.Lock()
	defer i.Unlock()

	cs := store.NewCommitingStore(i.store)

	_, refs, _ := chunk.Parts(cs.Chunk(cs.NextChunkAddress() - chunk.CommitChunkSize))

	m := modifier.New(cs, chunkSize, refs[0])

	err := t(m)
	if err != nil {
		return err
	}

	err = m.Error()

	if err != nil {
		return err
	}

	_, err = cs.Append(chunk.NewCommitChunk(m.RootAddress))
	if err != nil {
		return err
	}

	err = cs.Commit()
	if err != nil {
		return err
	}

	err = i.store.GC()

	if err != nil {
		return err
	}

	for _, l := range i.listeners {
		l.checkForChange(i)
	}
	return nil
}

func (i *ImmersaDB) ReadTransaction(m func(modifier.DBReader) error) error {
	i.RLock()
	defer i.RUnlock()
	return i.readTransaction(m)
}

func (i *ImmersaDB) readTransaction(t func(modifier.DBReader) error) error {
	_, refs, _ := chunk.Parts(i.store.Chunk(i.store.NextChunkAddress() - chunk.CommitChunkSize))
	m := modifier.New(i.store, chunkSize, refs[0])

	return t(m)
}

type Listener interface {
	OnChange(dbpath.Path, modifier.DBReader)
}

type listenerState struct {
	matcher     dbpath.Path
	listener    Listener
	latestState uint64
}

func (i *ImmersaDB) AddListener(matcher dbpath.Path, f Listener) {
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

type listenerFuncHolder func(dbpath.Path, modifier.DBReader)

func (l listenerFuncHolder) OnChange(p dbpath.Path, r modifier.DBReader) {
	l(p, r)
}

func (i *ImmersaDB) AddListenerFunc(matcher dbpath.Path, f func(dbpath.Path, modifier.DBReader)) {
	i.AddListener(matcher, listenerFuncHolder(f))
}

func (i *ImmersaDB) RemoveListenerFunc(matcher dbpath.Path, f func(dbpath.Path, modifier.DBReader)) {
	i.RemoveListener(matcher, listenerFuncHolder(f))
}

func (i *ImmersaDB) RemoveListener(matcher dbpath.Path, f Listener) {
	i.Lock()
	defer i.Unlock()

	log.Println("Listeners before", i.listeners)

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
	} else {
		log.Panic("Listener not found", matcher, f)
	}

	log.Println("Listeners after", i.listeners)

}

func (ls *listenerState) checkForChange(i *ImmersaDB) {
	i.readTransaction(func(r modifier.DBReader) error {
		if r.Exists(ls.matcher) {
			addr := r.AddressOf(ls.matcher)
			if addr != ls.latestState {

				ls.listener.OnChange(ls.matcher, r)
				ls.latestState = addr
			}
			return nil
		}
		return nil
	})

}
