package modifier

import (
	"encoding/binary"
	"errors"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
)

// DataWriter writes value chunked by max chunk size
type DataWriter struct {
	store        store.Store
	maxChunkSize int
	refs         []uint64
	current      []byte
	maxRefs      int
	maxDataSize  int
	bytesWritten uint64
}

// NewDataWriter creates new value writer that whill use given store and
// won't produce chunks larger than maxChunkSize
func NewDataWriter(s store.Store, maxChunkSize int) *DataWriter {

	maxRefs := (maxChunkSize - 4) / 8
	return &DataWriter{
		store:        s,
		maxChunkSize: maxChunkSize,
		maxRefs:      maxRefs,
	}
}

// ErrDataTooLarge is returned when the value being written is too large
var ErrDataTooLarge = errors.New("File too large")

// Write writes data to chunks. If the file is too big, it will return ErrDataTooLarge
func (v *DataWriter) Write(data []byte) (n int, err error) {
	v.current = append(v.current, data...)

	for len(v.current)+4 > v.maxChunkSize {
		ref, err := v.store.Append(chunk.Pack(chunk.DataType, nil, v.current[:v.maxChunkSize-4]))
		if err != nil {
			return 0, err
		}

		v.refs = append(v.refs, ref)

		if len(v.refs) > v.maxRefs {
			return 0, ErrDataTooLarge
		}
		v.current = v.current[v.maxChunkSize-4:]
	}

	v.bytesWritten += uint64(len(data))

	return len(data), nil
}

// Close writes the data header chunk and returns it's location.
func (v *DataWriter) Close() (uint64, error) {
	if len(v.current) > 0 {
		ref, err := v.store.Append(chunk.Pack(chunk.DataType, nil, v.current))
		if err != nil {
			return 0, err
		}

		v.refs = append(v.refs, ref)
	}

	sizeData := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeData, v.bytesWritten)

	ref, err := v.store.Append(chunk.Pack(chunk.DataHeaderType, v.refs, sizeData))
	if err != nil {
		return 0, err
	}

	return ref, nil
}
