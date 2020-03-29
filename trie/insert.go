package trie

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

func CreateEmpty(s store.Store) (store.Address, error) {
	tn := emptyNewTrieNode(nil)
	return tn.store(s, 0)
}

func Insert(s store.Store, root store.Address, key []byte, value store.Address) (store.Address, error) {

	sr := s.GetSegment(root)

	tr := trie(sr)

	if tr.count() == 0 {
		tn := emptyNewTrieNode()
		tn.prefix = key
		tn.value = value
		return tn.store(s, 0)
	}

	triePrefix := tr.getPrefix()

	cp, kp, pp := commonPrefix(key, triePrefix)

	if len(cp) == len(key) && len(pp) == 0 && len(kp) == 0 {
		nn := trieNodeCopy(tr)
		nn.value = value
		nn.prefix = key
		nn.count = tr.count() + 1
		return nn.store(s, 0)
	}

	if len(pp) > 0 {
		nn := emptyNewTrieNode()
		nn.prefix = cp
		nn.count = tr.count() + 1

		if len(kp) > 0 {
			idx := int(kp[0])
			nnn := emptyNewTrieNode()
			nnn.prefix = kp[1:]
			nnn.value = value
			nnn.count = 1

			chKey, err := nnn.store(s, 0)
			if err != nil {
				return store.NilAddress, errors.Wrap(err, "while creating child")
			}
			nn.children[idx] = chKey
		} else {
			nn.value = value
		}

		idx := int(pp[0])
		chKey, err := createTrieNodeCopy(s, rf, func(f TrieModifier) error {
			f.SetPrefix(pp[1:])
			return f.Error()
		})

		return createTrieNode(s, func(f TrieModifier) error {

			// f.SetPrefix(cp)

			// if len(kp) > 0 {
			// 	idx := int(kp[0])
			// 	chKey, err := createTrieNode(s, func(f TrieModifier) error {
			// 		f.SetPrefix(kp[1:])
			// 		f.SetChild(256, value)
			// 		return f.Error()
			// 	})

			// 	f.SetError(err)
			// 	f.SetChild(idx, chKey)
			// 	f.SetChild(256, store.NilAddress)
			// } else {
			// 	f.SetChild(256, value)
			// }

			// idx := int(pp[0])
			// chKey, err := createTrieNodeCopy(s, rf, func(f TrieModifier) error {
			// 	f.SetPrefix(pp[1:])
			// 	return f.Error()
			// })

			f.SetError(err)

			f.SetChild(idx, chKey)

			return f.Error()
		})
	}

	if len(pp) == 0 && len(kp) > 0 {
		return createTrieNodeCopy(s, rf, func(f TrieModifier) error {
			childIndex := int(kp[0])
			chk := f.GetChild(childIndex)
			if chk == store.NilAddress {
				chk, err = CreateEmpty(s)
				f.SetError(err)
			}
			nc, err := Insert(s, chk, kp[1:], value)
			f.SetError(err)
			f.SetChild(childIndex, nc)
			return f.Error()
		})
	}

	return store.NilAddress, errors.New("this part of trie.Insert should never be reached")
}

func commonPrefix(p1, p2 []byte) ([]byte, []byte, []byte) {

	maxIndex := len(p1)
	if len(p2) < maxIndex {
		maxIndex = len(p2)
	}

	for i := 0; i < maxIndex; i++ {
		if p1[i] != p2[i] {
			return p1[:i], p1[i:], p2[i:]
		}
	}

	return p1[:maxIndex], p1[maxIndex:], p2[maxIndex:]
}
