package store

type CommitingStore struct {
	store             BulkAppendStore
	chunks            [][]byte
	chunkByAddress    map[uint64][]byte
	storeStartAddress uint64
	lastChunkAddress  uint64
}

func NewCommitingStore(store BulkAppendStore) *CommitingStore {
	storeStartAddress := store.LastChunkAddress() + uint64(len(store.LastChunk())+8)

	return &CommitingStore{
		store:             store,
		storeStartAddress: storeStartAddress,
		chunkByAddress:    map[uint64][]byte{},
	}
}

func (cs *CommitingStore) Chunk(addr uint64) []byte {
	if addr < cs.storeStartAddress {
		return cs.store.Chunk(addr)
	}
	return cs.chunkByAddress[addr]
}

func (cs *CommitingStore) LastChunk() []byte {
	if cs.lastChunkAddress == 0 {
		return cs.store.LastChunk()
	}
	return cs.chunkByAddress[cs.lastChunkAddress]
}

func (cs *CommitingStore) LastChunkAddress() uint64 {
	if cs.lastChunkAddress == 0 {
		return cs.store.LastChunkAddress()
	}
	return cs.lastChunkAddress
}

func (cs *CommitingStore) BytesInStore() uint64 {
	bis := cs.store.BytesInStore()
	for _, c := range cs.chunks {
		bis += 8 + uint64(len(c))
	}
	return bis
}

func (cs *CommitingStore) Append(data []byte) (uint64, error) {

	if cs.lastChunkAddress == 0 {
		cs.lastChunkAddress = cs.storeStartAddress
	} else {
		ch := cs.chunkByAddress[cs.lastChunkAddress]
		cs.lastChunkAddress = cs.lastChunkAddress + uint64(len(ch)) + 8
	}

	cs.chunkByAddress[cs.lastChunkAddress] = data
	cs.chunks = append(cs.chunks, data)

	return cs.lastChunkAddress, nil
}

func (cs *CommitingStore) Commit() error {
	return cs.store.BulkAppend(cs.chunks)
}

func (cs *CommitingStore) Close() error {
	return cs.store.Close()
}
