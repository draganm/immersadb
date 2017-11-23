package modifier

import (
	"errors"
	"io"

	"github.com/draganm/immersadb/dbpath"
)

func NewMapModifierAdapter(m *Modifier) *MapModifierAdapter {
	return &MapModifierAdapter{
		m:    m,
		path: dbpath.New(),
	}
}

var ErrKeyDoesNotExist = errors.New("Key does not exist")

type MapModifierAdapter struct {
	m    *Modifier
	path dbpath.Path
}

func (m *MapModifierAdapter) InMap(key string, f func(ctx MapReader) error) error {

	newPath := m.path.Append(key)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	mm := &MapModifierAdapter{
		m:    m.m,
		path: newPath,
	}
	return f(mm)
}

func (m *MapModifierAdapter) InArray(key string, f func(ctx ArrayReader) error) error {

	newPath := m.path.Append(key)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	mm := &ArrayModifierAdapter{
		m:    m.m,
		path: newPath,
	}
	return f(mm)
}

func (m *MapModifierAdapter) ReadData(key string, f func(r io.Reader) error) error {
	newPath := m.path.Append(key)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	return f(m.m.EntityReaderFor(newPath).Data())
}

func (m *MapModifierAdapter) ForEach(f func(key string, t EntityType) error) error {
	return errors.New("Not supported")
}

func (m *MapModifierAdapter) ForEachAfter(key string, f func(index uint64, t EntityType) error) error {
	return errors.New("Not supported")
}

func (m *MapModifierAdapter) HasKey(key string) bool {
	return false
}

func (m *MapModifierAdapter) Type(key string) EntityType {
	return m.m.EntityReaderFor(m.path).Type()
}

func (m *MapModifierAdapter) Size() uint64 {
	return m.m.EntityReaderFor(m.path).Size()
}

func (m *MapModifierAdapter) CreateArray(key string, f func(ctx ArrayWriter) error) error {
	newPath := m.path.Append(key)
	err := m.m.CreateArray(newPath)
	if err != nil {
		return err
	}
	if f == nil {
		return nil
	}
	return f(&ArrayModifierAdapter{m.m, newPath})
}
func (m *MapModifierAdapter) ModifyArray(key string, f func(ctx ArrayWriter) error) error {
	newPath := m.path.Append(key)
	if !m.m.Exists(newPath) {
		return ErrNotFound
	}
	return f(&ArrayModifierAdapter{m.m, newPath})
}
func (m *MapModifierAdapter) CreateMap(key string, f func(ctx MapWriter) error) error {
	newPath := m.path.Append(key)
	return f(&MapModifierAdapter{m.m, newPath})
}
func (m *MapModifierAdapter) ModifyMap(key string, f func(ctx MapWriter) error) error {
	return errors.New("Not supported")
}
func (m *MapModifierAdapter) SetData(key string, f func(w io.Writer) error) error {
	newPath := m.path.Append(key)
	return m.m.CreateData(newPath, f)
}

func (m *MapModifierAdapter) DeleteKey(key string) error {
	return errors.New("Not supported")
}
func (m *MapModifierAdapter) DeleteAll() error {
	return errors.New("Not supported")
}