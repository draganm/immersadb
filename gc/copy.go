package gc

import (
	"crypto/sha256"
	"errors"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
)

// Copy copies content of one storage to the other in order to remove unused chunks.
// It will also de-dup leaves with the same content.
func Copy(source, destination store.Store) error {
	_, err := deepCopy(source.NextChunkAddress()-chunk.CommitChunkSize, source.NextChunkAddress(), map[uint64]uint64{}, map[string]uint64{}, source, destination)
	return err
}

func deepCopy(addr, beforeAddr uint64, addrMap map[uint64]uint64, contentMap map[string]uint64, source, destination store.Store) (bool, error) {

	ch := source.Chunk(addr)
	t, refs, data := chunk.Parts(ch)
	if len(refs) == 0 {

		s := sha256.Sum256(ch)

		contentAddr, found := contentMap[string(s[:])]

		if found {
			addrMap[addr] = contentAddr
			return true, nil
		}

		if addr >= beforeAddr {
			addrMap[addr] = addr
			contentMap[string(s[:])] = addr
			return false, nil
		}

		newAddr, err := destination.Append(ch)
		if err != nil {
			return false, err
		}

		addrMap[addr] = newAddr
		contentMap[string(s[:])] = newAddr
		return true, nil
	}

	didCopy := false

	for _, r := range refs {
		dc, err := deepCopy(r, beforeAddr, addrMap, contentMap, source, destination)
		didCopy = dc || didCopy
		if err != nil {
			return didCopy, err
		}
	}

	if !didCopy && addr >= beforeAddr {
		addrMap[addr] = addr
		return false, nil
	}

	for i, r := range refs {
		newRef, f := addrMap[r]
		if !f {
			// this should really not happen when DB is consistent
			return false, errors.New("Ref not found!")
		}

		refs[i] = newRef
	}

	ch = chunk.Pack(t, refs, data)
	newAddr, err := destination.Append(ch)

	addrMap[addr] = newAddr

	return true, err
}
