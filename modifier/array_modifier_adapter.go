package modifier

import (
	"errors"
	"io"

	"github.com/draganm/immersadb/dbpath"
)

func newArrayModifierAdapter(m *Modifier, path dbpath.Path) *ArrayModifierAdapter {
	return &ArrayModifierAdapter{
		m:    m,
		path: path,
	}
}

type ArrayModifierAdapter struct {
	m    *Modifier
	path dbpath.Path
}

func (m *ArrayModifierAdapter) InMap(index uint64, f func(ctx MapReader) error) error {

	if index >= m.Size() {
		return ErrIndexOutOfBounds
	}

	newPath := m.path.Append(index)

	if m.m.EntityReaderFor(newPath).Type() != Map {
		return ErrNotMap
	}

	mm := &MapModifierAdapter{
		m:    m.m,
		path: newPath,
	}
	return f(mm)
}

func (m *ArrayModifierAdapter) InArray(index uint64, f func(ctx ArrayReader) error) error {

	if index >= m.Size() {
		return ErrIndexOutOfBounds
	}

	newPath := m.path.Append(index)

	if m.m.EntityReaderFor(newPath).Type() != Array {
		return ErrNotArray
	}
	mm := &ArrayModifierAdapter{
		m:    m.m,
		path: newPath,
	}
	return f(mm)
}

func (m *ArrayModifierAdapter) ReadData(index uint64, f func(r io.Reader) error) error {

	if index >= m.Size() {
		return ErrIndexOutOfBounds
	}

	newPath := m.path.Append(index)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	if m.m.EntityReaderFor(newPath).Type() != Data {
		return ErrNotData
	}

	return f(m.m.EntityReaderFor(newPath).Data())
}

func (m *ArrayModifierAdapter) ForEach(f func(index uint64, t EntityType) error) error {
	return m.m.EntityReaderFor(m.path).ForEachArrayElement(func(index uint64, reader EntityReader) error {
		return f(index, reader.Type())
	})
}

func (m *ArrayModifierAdapter) ForEachAfter(index uint64, f func(index uint64, t EntityType) error) error {
	return errors.New("Not supported")
}

func (m *ArrayModifierAdapter) Type(index uint64) EntityType {
	newPath := m.path.Append(index)

	if !m.m.Exists(newPath) {
		return Unknown
	}

	return m.m.EntityReaderFor(newPath).Type()
}

func (m *ArrayModifierAdapter) Size() uint64 {
	return m.m.EntityReaderFor(m.path).Size()
}

func (m *ArrayModifierAdapter) PrependArray(f func(ctx ArrayWriter) error) error {
	newPath := m.path.Append(uint64(0))
	err := m.m.CreateArray(newPath)
	if err != nil {
		return err
	}
	if f == nil {
		return nil
	}
	return f(&ArrayModifierAdapter{m.m, newPath})
}

func (m *ArrayModifierAdapter) ModifyArray(index uint64, f func(ctx ArrayWriter) error) error {
	if index >= m.Size() {
		return ErrIndexOutOfBounds
	}

	if m.Type(index) != Array {
		return ErrNotArray
	}

	if f == nil {
		return nil
	}
	newPath := m.path.Append(index)

	return f(&ArrayModifierAdapter{m.m, newPath})

}

func (m *ArrayModifierAdapter) PrependMap(f func(ctx MapWriter) error) error {
	newPath := m.path.Append(uint64(0))
	err := m.m.CreateMap(newPath)
	if err != nil {
		return err
	}
	if f == nil {
		return nil
	}
	return f(&MapModifierAdapter{m.m, newPath})
}

func (m *ArrayModifierAdapter) ModifyMap(index uint64, f func(ctx MapWriter) error) error {
	if index >= m.Size() {
		return ErrIndexOutOfBounds
	}

	if m.Type(index) != Map {
		return ErrNotMap
	}

	if f == nil {
		return nil
	}
	newPath := m.path.Append(index)

	return f(&MapModifierAdapter{m.m, newPath})
}

func (m *ArrayModifierAdapter) PrependData(f func(w io.Writer) error) error {
	newPath := m.path.Append(uint64(0))
	err := m.m.CreateData(newPath, f)
	if err != nil {
		return err
	}
	return nil
}

func (m *ArrayModifierAdapter) SetData(index uint64, f func(w io.Writer) error) error {
	if index >= m.Size() {
		return ErrIndexOutOfBounds
	}

	if f == nil {
		return nil
	}
	newPath := m.path.Append(index)

	return m.m.CreateData(newPath, f)

}

func (m *ArrayModifierAdapter) DeleteLast() error {
	size := m.m.EntityReaderFor(m.path).Size()
	if size == 0 {
		return ErrArrayEmpty
	}
	newPath := m.path.Append(size - 1)
	return m.m.Delete(newPath)
}

func (m *ArrayModifierAdapter) DeleteAll() error {
	return m.m.CreateArray(m.path)
}
