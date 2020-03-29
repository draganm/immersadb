package trie

import (
	"bytes"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func ForEachAfter(s store.Store, root store.Address, after []byte, f func(key []byte, value store.Address) error) error {
	err := forEachAfter(s, root, after, nil, f)
	if err == StopIteration {
		return nil
	}

	return err
}

func forEachAfter(s store.Store, root store.Address, after []byte, prefix []byte, f func(key []byte, value store.Address) error) error {
	fr, err := s.Get(root)
	if err != nil {
		return errors.Wrap(err, "while getting trie fragment")
	}

	tm := NewTrieModifier(fr)
	tp := tm.GetPrefix()

	_, ap, tpp := commonPrefix(after, tp)

	cmp := bytes.Compare(ap, tpp)

	if cmp == 0 {

		for i := 0; i < 256; i++ {

			npr := append(append(prefix, tp...), byte(i))

			chk := tm.GetChild(i)

			if tm.Error() != nil {
				return tm.Error()
			}

			if chk != store.NilAddress {
				err = forEach(s, chk, npr, f)
			}

			if err != nil {
				return err
			}

		}

		return nil
	}

	if cmp <= 0 {
		return forEach(s, root, prefix, f)
	}

	if len(tpp) == 0 {

		fromIdx := int(ap[0])

		naf := ap[1:]

		chk := tm.GetChild(fromIdx)

		if tm.Error() != nil {
			return tm.Error()
		}

		if chk != store.NilAddress {
			npr := append(append(prefix, tp...), byte(fromIdx))

			err = forEachAfter(s, chk, naf, npr, f)
			if err != nil {
				return err
			}

		}

		for i := fromIdx + 1; i < 256; i++ {

			npr := append(append(prefix, tp...), byte(i))

			chk := tm.GetChild(i)

			if tm.Error() != nil {
				return tm.Error()
			}

			if chk != store.NilAddress {
				err = forEach(s, chk, npr, f)
				if err != nil {
					return err
				}

			}

		}

		return nil
	}

	return nil
}
