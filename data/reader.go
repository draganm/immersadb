package data

import (
	"io"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type reader struct {
	store        store.Store
	path         []int
	root         store.Address
	currentBlock []byte
}

func NewReader(root store.Address, store store.Store) (io.Reader, error) {
	r := &reader{
		store: store,
		root:  root,
	}

	err := r.firstBlock()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	if len(r.currentBlock) == 0 {
		err := r.nextBlock()
		if err != nil {
			return 0, err
		}

	}

	n = len(p)
	if n > len(r.currentBlock) {
		n = len(r.currentBlock)
	}

	copy(p, r.currentBlock[:n])
	r.currentBlock = r.currentBlock[n:]
	return n, nil

}

func (r *reader) nextBlock() error {

	if len(r.path) == 0 {
		return io.EOF
	}

	r.path[len(r.path)-1]++

	keys := make([]store.Address, len(r.path)+1, len(r.path)+1)
	keys[0] = r.root

	for i := 0; ; i++ {
		sr := r.store.GetSegment(keys[i])

		switch sr.Type() {
		case store.TypeDataNode:

			if sr.NumberOfChildren() == 0 {
				return errors.Errorf("found data node with 0 children")
			}

			idx := r.path[i]

			if idx >= sr.NumberOfChildren() {
				// oops, drop last, increase second but last

				if i == 0 {
					return io.EOF
				}

				r.path[i] = 0

				i--
				r.path[i]++
				i--
				continue
			}

			kb := sr.GetChildAddress(idx)
			keys[i+1] = store.Address(kb)

		case store.TypeDataLeaf:

			r.currentBlock = sr.GetData()

			return nil

		default:
			return errors.Errorf("Unexpected segment while reading data %s", sr.Type())
		}
	}

}

func (r *reader) firstBlock() error {

	k := r.root

	for {
		sr := r.store.GetSegment(k)

		switch sr.Type() {
		case store.TypeDataNode:
			r.path = append(r.path, 0)

			if sr.NumberOfChildren() == 0 {
				return errors.Errorf("found data node with 0 children")
			}

			kb := sr.GetChildAddress(0)
			k = store.Address(kb)

		case store.TypeDataLeaf:

			r.currentBlock = sr.GetData()

			return nil

		default:
			return errors.Errorf("Unexpected segment while reading data %q", sr.Type())
		}
	}

}
