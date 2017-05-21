package gc

import (
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
)

func Size(s store.Store) uint64 {
	done := map[uint64]struct{}{}
	toDo := []uint64{s.LastChunkAddress()}
	size := uint64(0)

	for len(toDo) > 0 {

		next := toDo[0]
		toDo = toDo[1:]

		c := s.Chunk(next)
		_, refs, data := chunk.Parts(c)
		s := uint64(len(refs)*8 + len(data) + 8 + 2 + 2)
		size += s
		done[next] = struct{}{}

		for _, r := range refs {
			_, found := done[r]
			if !found {
				toDo = append(toDo, r)
			}
		}

	}
	return size

}
