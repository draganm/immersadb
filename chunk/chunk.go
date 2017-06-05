package chunk

import "encoding/binary"

type ChunkType uint16

const ErrorType ChunkType = 0
const CommitType ChunkType = 1
const DataType ChunkType = 10
const DataHeaderType ChunkType = 11
const ArrayLeafType ChunkType = 30
const ArrayNodeType ChunkType = 31

const TTFMapNode ChunkType = 40

const CommitChunkSize = uint64(2 + 2 + 8)

func Type(chunk []byte) ChunkType {
	if len(chunk) < 1 {
		return ErrorType
	}

	return ChunkType(chunk[0])
}

// Parts returns the type of the chunk, references and data contained in the chunk.
func Parts(chunk []byte) (ChunkType, []uint64, []byte) {
	if len(chunk) < 1 {
		return ErrorType, nil, nil
	}

	t := chunk[0]
	if len(chunk) < 2 {
		return ChunkType(t), nil, nil
	}

	refCount := int(chunk[1]) & 0xff

	if len(chunk) < refCount*8+2 {
		return ErrorType, nil, nil
	}
	refs := make([]uint64, refCount)

	for i := 0; i < refCount; i++ {
		refs[i] = binary.BigEndian.Uint64(chunk[2+i*8:])
	}

	data := chunk[2+refCount*8:]

	return ChunkType(t), refs, data

}

// Pack creates a byte array representing chunk.
func Pack(t ChunkType, refs []uint64, data []byte) []byte {
	chunk := make([]byte, 2+len(refs)*8+len(data))
	chunk[0] = byte(t)
	chunk[1] = byte(len(refs))
	for i, r := range refs {
		binary.BigEndian.PutUint64(chunk[2+i*8:], r)
	}
	copy(chunk[2+len(refs)*8:], data)
	return chunk
}
