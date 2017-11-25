package store

import (
	"github.com/draganm/immersadb/chunk"
)

func Evacuate(s Store, addr uint64) error {
	_, err := deepCopy(s.NextChunkAddress()-chunk.CommitChunkSize, addr, map[uint64]uint64{}, map[string]uint64{}, s, s)
	return err
}
