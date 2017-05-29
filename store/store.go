package store

// Store is an interface representing a store.
// Usually store is a mmaped file or group of files.
type Store interface {
	Append(data []byte) (uint64, error)
	Chunk(addr uint64) []byte
	BytesInStore() uint64
	NextChunkAddress() uint64
	Close() error
}

type BulkAppendStore interface {
	Store
	BulkAppend(chunks [][]byte) error
}
