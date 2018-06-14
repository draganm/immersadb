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

type DBReader interface {
	AddressOf(p dbpath.Path) uint64
	TypeOf(p dbpath.Path) EntityType
	Exists(p dbpath.Path) bool
	ForEach(p dbpath.Path, f func(p dbpath.Path) bool)
	ForEachAfter(p dbpath.Path, f func(p dbpath.Path) bool)
	Read(p dbpath.Path, f func(io.Reader) error)
	AbortIfError(error)
}

type DBWriter interface {
	DBReader
	CreateMap(p dbpath.Path)
	CreateArray(p dbpath.Path)
	CreateData(p dbpath.Path, f func(w io.Writer) error)
	Delete(p dbpath.Path)
}
