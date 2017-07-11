package dbpath

type Path []interface{}

func New(path ...interface{}) Path {
	return Path(path)
}
