package vfs

import (
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/draganm/immersadb"
	"github.com/draganm/immersadb/modifier"
)

// ImmersaFS implements https://golang.org/pkg/net/http/#File
type ImmersaFS struct {
	db *immersadb.ImmersaDB
}

func New(db *immersadb.ImmersaDB) *ImmersaFS {
	return &ImmersaFS{
		db: db,
	}
}

type DBFile struct {
	db   *immersadb.ImmersaDB
	path modifier.DBPath
}

func (i *ImmersaFS) Open(name string) (http.File, error) {
	path := modifier.DBPath{}

	err := i.db.ReadTransaction(func(r modifier.EntityReader) error {
		parts := strings.Split(name, "/")
		// var err error
		for _, p := range parts {
			sr, err := r.EntityReaderFor(path)
			if err != nil {
				return err
			}
			switch sr.Type() {
			case modifier.Hash:
				path = append(path, p)
			case modifier.Array:
				idx, err := strconv.ParseUint(p, 10, 64)
				if err != nil {
					return err
				}
				path = append(path, int(idx))
			default:
				return errors.New("Wrong path")
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &DBFile{
		db:   i.db,
		path: path,
	}, nil
}

func (f *DBFile) Close() error {
	return nil
}

func (f *DBFile) Read(p []byte) (n int, err error) {
	return 0, errors.New("Read not supported")
}

func (f *DBFile) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("Seek Not supported")
}

func (f *DBFile) Readdir(count int) ([]os.FileInfo, error) {

}

func (f *DBFile) Stat() (os.FileInfo, error) {

}

// type File interface {
//         io.Closer
//         io.Reader
//         io.Seeker
//         Readdir(count int) ([]os.FileInfo, error)
//         Stat() (os.FileInfo, error)
// }
