package chunk

import (
	"github.com/draganm/immersadb/store"
)

func NewCommitChunk(rootAddr uint64) []byte {
	return Pack(CommitType, []uint64{rootAddr}, nil)
}

func LastCommitRootHashAddress(s store.Store) uint64 {
	_, refs, _ := Parts(s.Chunk(s.NextChunkAddress() - CommitChunkSize))
	return refs[0]
}
