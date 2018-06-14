package dbpath

import (
	"fmt"
	"strconv"
	"strings"
)

type Path []interface{}

func New(path ...interface{}) Path {
	return Path(path)
}

func P(pth string) Path {
	p := New()
	for _, e := range strings.Split(pth, "/") {
		if strings.HasPrefix(e, "#") {
			idx, err := strconv.ParseUint(e[1:], 10, 64)
			if err != nil {
				p = p.Append(e)
			} else {
				p = p.Append(idx)
			}
		} else {
			p = p.Append(e)
		}
	}
	return p
}

func (p Path) Append(element interface{}) Path {
	switch element.(type) {
	case int:
		return append(p, uint64(element.(int)))
	case string, uint64:
		return append(p, element)
	default:
		panic(fmt.Errorf("Wrong element type: %#v", element))
	}
}

func (p Path) Last() interface{} {
	if len(p) == 0 {
		return nil
	}
	return p[len(p)-1]
}
