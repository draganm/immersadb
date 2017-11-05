package store

import "github.com/draganm/immersadb/chunk"

// Store is an interface representing a store.
// Usually store is a mmaped file or group of files.
type Store interface {
	Append(data []byte) (uint64, error)
	Chunk(addr uint64) []byte
	BytesInStore() uint64
	NextChunkAddress() uint64
	FirstChunkAddress() uint64
	Close() error
}

type BulkAppendStore interface {
	Store
	BulkAppend(chunks [][]byte) error
}

func LastCommitRootHashAddress(s Store) uint64 {
	_, refs, _ := chunk.Parts(s.Chunk(s.NextChunkAddress() - chunk.CommitChunkSize))
	return refs[0]
}
