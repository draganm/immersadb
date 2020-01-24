package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func CreateEmpty(s store.Store) (store.Address, error) {
	nm, err := newNodeModifier(s, nil)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node modifier")
	}
	return nm.Address, nil
}
