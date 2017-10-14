package store

type CommitingStore struct {
	store             BulkAppendStore
	chunks            [][]byte
	chunkByAddress    map[uint64][]byte
	storeStartAddress uint64
	storeEndAddress   uint64
}

func (s *CommitingStore) FirstChunkAddress() uint64 {
	return s.store.FirstChunkAddress()
}

func NewCommitingStore(store BulkAppendStore) *CommitingStore {
	storeStartAddress := store.NextChunkAddress()

	return &CommitingStore{
		store:             store,
		storeStartAddress: storeStartAddress,
		chunkByAddress:    map[uint64][]byte{},
		storeEndAddress:   storeStartAddress,
	}
}

func (cs *CommitingStore) Chunk(addr uint64) []byte {
	if addr < cs.storeStartAddress {
		return cs.store.Chunk(addr)
	}
	return cs.chunkByAddress[addr]
}

func (cs *CommitingStore) BytesInStore() uint64 {
	bis := cs.store.BytesInStore()
	for _, c := range cs.chunks {
		bis += 2 + uint64(len(c))
	}
	return bis
}

func (cs *CommitingStore) NextChunkAddress() uint64 {
	return cs.storeEndAddress
}

func (cs *CommitingStore) Append(data []byte) (uint64, error) {
	last := cs.storeEndAddress
	cs.chunkByAddress[last] = data
	cs.chunks = append(cs.chunks, data)
	cs.storeEndAddress += uint64(len(data) + 2)
	return last, nil
}

func (cs *CommitingStore) Commit() error {
	return cs.store.BulkAppend(cs.chunks)
}

func (cs *CommitingStore) Close() error {
	return cs.store.Close()
}
