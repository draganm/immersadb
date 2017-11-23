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

// var ErrKeyDoesNotExist = errors.New("Key does not exist")

type ArrayModifierAdapter struct {
	m    *Modifier
	path dbpath.Path
}

func (m *ArrayModifierAdapter) InMap(index uint64, f func(ctx MapReader) error) error {

	newPath := m.path.Append(index)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	mm := &MapModifierAdapter{
		m:    m.m,
		path: newPath,
	}
	return f(mm)
}

func (m *ArrayModifierAdapter) InArray(index uint64, f func(ctx ArrayReader) error) error {

	newPath := m.path.Append(index)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	mm := &ArrayModifierAdapter{
		m:    m.m,
		path: newPath,
	}
	return f(mm)
}

func (m *ArrayModifierAdapter) ReadData(index uint64, f func(r io.Reader) error) error {
	newPath := m.path.Append(index)
	if !m.m.Exists(newPath) {
		return ErrKeyDoesNotExist
	}

	return f(m.m.EntityReaderFor(newPath).Data())
}

func (m *ArrayModifierAdapter) ForEach(f func(index uint64, t EntityType) error) error {
	return errors.New("Not supported")
}

func (m *ArrayModifierAdapter) ForEachAfter(index uint64, f func(index uint64, t EntityType) error) error {
	return errors.New("Not supported")
}

func (m *ArrayModifierAdapter) HasKey(index uint64) bool {
	return false
}

func (m *ArrayModifierAdapter) Type(index uint64) EntityType {
	return m.m.EntityReaderFor(m.path).Type()
}

func (m *ArrayModifierAdapter) Size() uint64 {
	return m.m.EntityReaderFor(m.path).Size()
}

func (m *ArrayModifierAdapter) AppendArray(f func(ctx ArrayWriter) error) (uint64, error) {
	return 0, errors.New("Not supported")
}

func (m *ArrayModifierAdapter) ModifyArray(index uint64, f func(ctx ArrayWriter) error) error {
	return errors.New("Not supported")
}

//
func (m *ArrayModifierAdapter) AppendMap(f func(ctx MapWriter) error) (uint64, error) {
	return 0, errors.New("Not supported")
}

func (m *ArrayModifierAdapter) ModifyMap(index uint64, f func(ctx MapWriter) error) error {
	return errors.New("Not supported")
}

//
func (m *ArrayModifierAdapter) AppendData(f func(w io.Writer) error) (uint64, error) {
	newPath := m.path.Append(0)
	err := m.m.CreateData(newPath, f)
	if err != nil {
		return 0, err
	}
	size := m.m.EntityReaderFor(m.path).Size()
	return size, nil
}

func (m *ArrayModifierAdapter) SetData(index uint64, f func(w io.Writer) error) error {
	return errors.New("Not supported")
}

//
func (m *ArrayModifierAdapter) DeleteLast() error {
	size := m.m.EntityReaderFor(m.path).Size()
	newPath := m.path.Append(size - 1)
	return m.m.Delete(newPath)
}

//
func (m *ArrayModifierAdapter) DeleteAll() error {
	return errors.New("Not supported")
}

// func (m *ArrayModifierAdapter) DeleteFirst() error {
// 	return errors.New("Not supported")
// }