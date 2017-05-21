package gc

import (
	"crypto/sha256"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
)

// Copy copies content of one storage to the other in order to remove unused chunks.
// It will also de-dup leaves with the same content.
func Copy(source, destination store.Store) error {
	return deepCopy(source.LastChunkAddress(), map[uint64]uint64{}, map[string]uint64{}, source, destination)
}

func deepCopy(addr uint64, addrMap map[uint64]uint64, contentMap map[string]uint64, source, destination store.Store) error {
	ch := source.Chunk(addr)
	t, refs, data := chunk.Parts(ch)
	if len(refs) == 0 {

		s := sha256.Sum256(ch)

		contentAddr, found := contentMap[string(s[:])]

		if found {
			addrMap[addr] = contentAddr
			return nil
		}

		newAddr, err := destination.Append(ch)
		if err != nil {
			return err
		}
		addrMap[addr] = newAddr
		contentMap[string(s[:])] = newAddr
		return nil
	}
	for _, r := range refs {
		err := deepCopy(r, addrMap, contentMap, source, destination)
		if err != nil {
			return err
		}
	}

	for i, r := range refs {
		refs[i] = addrMap[r]
	}

	ch = chunk.Pack(t, refs, data)
	newAddr, err := destination.Append(ch)

	addrMap[addr] = newAddr

	return err
}
