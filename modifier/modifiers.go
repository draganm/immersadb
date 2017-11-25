package modifier

import (
	"errors"
	"io"

	"github.com/draganm/immersadb/dbpath"
)

type EntityType uint16

const Data EntityType = 0
const Map EntityType = 1
const Array EntityType = 2
const Unknown EntityType = 0xffff

var ErrNotMap = errors.New("Not a map")
var ErrNotArray = errors.New("Not an array")
var ErrNotData = errors.New("Not data")

var ErrKeyDoesNotExist = errors.New("Key does not exist")
var ErrKeyAlreadyExists = errors.New("Key already exists")

var ErrIndexOutOfBounds = errors.New("Index out of bounds")
var ErrArrayEmpty = errors.New("Array is empty")

type ArrayReader interface {
	InArray(index uint64, f func(m ArrayReader) error) error
	InMap(index uint64, f func(m MapReader) error) error
	ReadData(index uint64, f func(r io.Reader) error) error
	ForEach(f func(index uint64, t EntityType) error) error
	ForEachAfter(from uint64, f func(index uint64, t EntityType) error) error

	Type(index uint64) EntityType
	Size() uint64
}

type MapReader interface {
	InArray(key string, f func(m ArrayReader) error) error
	InMap(key string, f func(m MapReader) error) error
	ReadData(key string, f func(r io.Reader) error) error
	ForEach(f func(key string, t EntityType) error) error
	ForEachAfter(key string, f func(index uint64, t EntityType) error) error

	HasKey(key string) bool
	Type(key string) EntityType
	Size() uint64
}

type ArrayWriter interface {
	ArrayReader

	PrependArray(f func(m ArrayWriter) error) error
	ModifyArray(index uint64, f func(m ArrayWriter) error) error

	PrependMap(f func(m MapWriter) error) error
	ModifyMap(index uint64, f func(m MapWriter) error) error

	PrependData(f func(w io.Writer) error) error
	SetData(index uint64, f func(w io.Writer) error) error

	// DeleteFirst() error
	DeleteLast() error

	DeleteAll() error
}

type MapWriter interface {
	MapReader
	CreateArray(key string, f func(m ArrayWriter) error) error
	ModifyArray(key string, f func(m ArrayWriter) error) error

	CreateMap(key string, f func(m MapWriter) error) error
	ModifyMap(key string, f func(m MapWriter) error) error

	SetData(key string, f func(w io.Writer) error) error

	DeleteKey(key string) error
	DeleteAll() error
}

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
