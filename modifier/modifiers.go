package modifier

import (
	"io"

	"github.com/draganm/immersadb/dbpath"
)

type EntityType uint16

const Data EntityType = 0
const Map EntityType = 1
const Array EntityType = 2
const Unknown EntityType = 0xffff

type EntityReader interface {
	Size() uint64
	Address() uint64
	Type() EntityType
	EntityReaderFor(path dbpath.Path) EntityReader
	Data() io.Reader
	ForEachMapEntry(func(key string, reader EntityReader) error) error
	Exists(path dbpath.Path) bool
	ForEachArrayElement(func(index uint64, reader EntityReader) error) error
}

type EntityWriter interface {
	Delete(path dbpath.Path) error
	CreateData(path dbpath.Path, f func(io.Writer) error) error
	CreateArray(path dbpath.Path) error
	CreateMap(path dbpath.Path) error
	EntityReader
}
