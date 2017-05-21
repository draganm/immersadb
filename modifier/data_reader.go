package modifier

import (
	"errors"
	"io"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
)

// ErrNotDataChunk is returned when a non-data chunk is ecountered.
var ErrNotDataChunk = errors.New("Not DataType chunk")

// DataReader is constant memory limited implementation of io.Reader.
// It reads data from the chunks in the store.
type DataReader struct {
	store               store.Store
	currentData         []byte
	currentDataChunkIdx int
	posInChunk          int
	chunks              []uint64
}

// NewDataReader creates a new instance of value reader for given store
// and chunk address.
func NewDataReader(s store.Store, chunkAddr uint64) (*DataReader, error) {

	t, refs, _ := chunk.Parts(s.Chunk(chunkAddr))

	if t != chunk.DataHeaderType {
		return nil, errors.New("Not DataHeader chunk")
	}

	t, _, data := chunk.Parts(s.Chunk(refs[0]))
	if t != chunk.DataType {
		return nil, ErrNotDataChunk
	}

	return &DataReader{
		store:               s,
		currentDataChunkIdx: 0,
		posInChunk:          0,
		currentData:         data,
		chunks:              refs,
	}, nil
}

// Read reads next bytes from the value or return io.EOF.
func (v *DataReader) Read(p []byte) (n int, err error) {
	toCopy := len(p)
	if v.posInChunk+toCopy > len(v.currentData) {
		toCopy = len(v.currentData) - v.posInChunk
	}

	if toCopy == 0 && v.currentDataChunkIdx+1 < len(v.chunks) {
		v.currentDataChunkIdx++
		t, _, data := chunk.Parts(v.store.Chunk(v.chunks[v.currentDataChunkIdx]))
		if t != chunk.DataType {
			return 0, ErrNotDataChunk
		}
		v.currentData = data
		v.posInChunk = 0
		toCopy = len(v.currentData) - v.posInChunk
	}

	if toCopy == 0 {
		return 0, io.EOF
	}

	copy(p, v.currentData[v.posInChunk:v.posInChunk+toCopy])
	v.posInChunk += toCopy
	return toCopy, nil
}
