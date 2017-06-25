package modifier

import (
	"io"
)

type DBPath []interface{}

func Path(path ...interface{}) DBPath {
	return DBPath(path)
}

type EntityType uint16

const Data EntityType = 0
const Map EntityType = 1
const Array EntityType = 2
const Unknown EntityType = 0xffff

type EntityReader interface {
	Size() uint64
	Address() uint64
	Type() EntityType
	EntityReaderFor(path DBPath) (EntityReader, error)
	Data() (io.Reader, error)
	ForEachMapEntry(func(key string, reader EntityReader) error) error
	Exists(path DBPath) bool
	ForEachArrayElement(func(index uint64, reader EntityReader) error) error
}

type EntityWriter interface {
	Delete(path DBPath) error
	CreateData(path DBPath, f func(io.Writer) error) error
	CreateArray(path DBPath) error
	CreateMap(path DBPath) error
	EntityReader
}
