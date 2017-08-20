package dbpath

type Path []interface{}

func New(path ...interface{}) Path {
	return Path(path)
}

func (p Path) Append(element interface{}) Path {
	return Path{append(p, element)}
}
