package chunk

import "encoding/binary"

type ChunkType uint16

const ErrorType ChunkType = 0
const CommitType ChunkType = 1
const DataType ChunkType = 10
const DataHeaderType ChunkType = 11
const HashLeafType ChunkType = 20
const HashNodeType ChunkType = 21
const ArrayLeafType ChunkType = 30
const ArrayNodeType ChunkType = 31

const CommitChunkSize = uint64(8 + 2 + 2 + 8)

func Type(chunk []byte) ChunkType {
	if len(chunk) < 2 {
		return ErrorType
	}

	return ChunkType(binary.BigEndian.Uint16(chunk))
}

// Parts returns the type of the chunk, references and data contained in the chunk.
func Parts(chunk []byte) (ChunkType, []uint64, []byte) {
	if len(chunk) < 2 {
		return ErrorType, nil, nil
	}

	t := binary.BigEndian.Uint16(chunk)
	if len(chunk) < 4 {
		return ChunkType(t), nil, nil
	}

	refCount := int(binary.BigEndian.Uint16(chunk[2:]))

	if len(chunk) < refCount*8+4 {
		return ErrorType, nil, nil
	}
	refs := make([]uint64, refCount)

	for i := 0; i < refCount; i++ {
		refs[i] = binary.BigEndian.Uint64(chunk[4+i*8:])
	}

	data := chunk[4+refCount*8:]

	return ChunkType(t), refs, data

}

// Pack creates a byte array representing chunk.
func Pack(t ChunkType, refs []uint64, data []byte) []byte {
	chunk := make([]byte, 4+len(refs)*8+len(data))
	binary.BigEndian.PutUint16(chunk, uint16(t))
	binary.BigEndian.PutUint16(chunk[2:], uint16(len(refs)))
	for i, r := range refs {
		binary.BigEndian.PutUint64(chunk[4+i*8:], r)
	}
	copy(chunk[4+len(refs)*8:], data)
	return chunk
}
