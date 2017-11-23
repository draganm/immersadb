package dbpath

import "fmt"

type Path []interface{}

func New(path ...interface{}) Path {
	return Path(path)
}

func (p Path) Append(element interface{}) Path {
	switch element.(type) {
	case string, int, uint64:
		return append(p, element)
	default:
		panic(fmt.Errorf("Wrong element type: %#v", element))
	}

}
