package trie

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

var StopIteration = errors.New("stop iteration")

func ForEach(s store.Store, root store.Address, f func(key []byte, value store.Address) error) error {
	err := forEach(s, root, nil, f)
	if err == StopIteration {
		return nil
	}
	return err
}

func forEach(s store.Store, root store.Address, prefix []byte, f func(key []byte, value store.Address) error) error {
	fr, err := s.Get(root)
	if err != nil {
		return errors.Wrap(err, "while getting trie fragment")
	}

	tm := NewTrieModifier(fr)
	tp := tm.GetPrefix()

	vk := tm.GetChild(256)

	if tm.Error() != nil {
		return tm.Error()
	}

	key := make([]byte, len(prefix))
	copy(key, prefix)
	key = append(key, tp...)

	if vk != store.NilAddress {
		err = f(key, vk)
		if err != nil {
			return err
		}
	}

	for i := 0; i < 256; i++ {
		ck := tm.GetChild(i)
		if tm.Error() != nil {
			return tm.Error()
		}

		chPrefix := append(key, byte(i))

		if ck != store.NilAddress {
			err = forEach(s, ck, chPrefix, f)
			if err != nil {
				return err
			}
		}
	}

	return nil

}
