package store

import (
	"log"

	"github.com/draganm/immersadb/chunk"
)

func Dump(s Store) {
	log.Println("-- DUMP")
	lastAddress := s.NextChunkAddress()
	for addr := s.FirstChunkAddress(); addr < lastAddress; {
		log.Println("- ADDR:", addr)
		cd := s.Chunk(addr)
		addr += 2 + uint64(len(cd))
		t, refs, data := chunk.Parts(cd)
		log.Println("Type", t)
		log.Println("REFS", refs)
		log.Println("DATA", data)
	}
}
