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

type MapModifierAdapter struct {
	m    *Modifier
	path dbpath.Path
}

func (m *MapModifierAdapter) InMap(key string, f func(ctx MapReader) error) error {

	newPath := m.path.Append(key)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	if m.m.EntityReaderFor(newPath).Type() != Map {
		return ErrNotMap
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

	if m.m.EntityReaderFor(newPath).Type() != Array {
		return ErrNotArray
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

	if m.m.EntityReaderFor(newPath).Type() != Data {
		return ErrNotData
	}

	return f(m.m.EntityReaderFor(newPath).Data())
}

func (m *MapModifierAdapter) ForEach(f func(key string, t EntityType) error) error {
	return m.m.EntityReaderFor(m.path).ForEachMapEntry(func(key string, reader EntityReader) error {
		return f(key, reader.Type())
	})
}

func (m *MapModifierAdapter) ForEachAfter(key string, f func(index uint64, t EntityType) error) error {
	return errors.New("Not supported")
}

func (m *MapModifierAdapter) HasKey(key string) bool {
	newPath := m.path.Append(key)
	return m.m.Exists(newPath)
}

func (m *MapModifierAdapter) Type(key string) EntityType {
	newPath := m.path.Append(key)

	if !m.m.Exists(newPath) {
		return Unknown
	}

	return m.m.EntityReaderFor(newPath).Type()
}

func (m *MapModifierAdapter) Size() uint64 {
	count := uint64(0)
	m.m.ForEachMapEntry(func(key string, reader EntityReader) error {
		count++
		return nil
	})
	return count
}

func (m *MapModifierAdapter) CreateArray(key string, f func(ctx ArrayWriter) error) error {
	newPath := m.path.Append(key)

	if m.m.HasPath(newPath) {
		return ErrKeyAlreadyExists
	}

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
		return ErrKeyDoesNotExist
	}

	if m.m.EntityReaderFor(newPath).Type() != Array {
		return ErrNotArray
	}

	return f(&ArrayModifierAdapter{m.m, newPath})
}

func (m *MapModifierAdapter) CreateMap(key string, f func(ctx MapWriter) error) error {
	newPath := m.path.Append(key)

	if m.m.HasPath(newPath) {
		return ErrKeyAlreadyExists
	}

	err := m.m.CreateMap(newPath)
	if err != nil {
		return err
	}

	if f == nil {
		return nil
	}
	return f(&MapModifierAdapter{m.m, newPath})
}
func (m *MapModifierAdapter) ModifyMap(key string, f func(ctx MapWriter) error) error {
	newPath := m.path.Append(key)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	if m.m.EntityReaderFor(newPath).Type() != Map {
		return ErrNotMap
	}

	return f(&MapModifierAdapter{m.m, newPath})
}
func (m *MapModifierAdapter) SetData(key string, f func(w io.Writer) error) error {
	newPath := m.path.Append(key)

	if m.m.HasPath(newPath) && m.m.EntityReaderFor(newPath).Type() != Data {
		return ErrNotData
	}

	return m.m.CreateData(newPath, f)

}

func (m *MapModifierAdapter) DeleteKey(key string) error {
	newPath := m.path.Append(key)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}
	return m.m.Delete(newPath)
}
func (m *MapModifierAdapter) DeleteAll() error {
	return m.m.clearMap(m.path)
}
