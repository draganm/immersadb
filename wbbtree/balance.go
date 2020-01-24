package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

const weight = 4

func balance(s store.Store, k store.Address) (store.Address, error) {
	if k == store.NilAddress {
		return k, nil

	}
	nr, err := newNodeReader(s, k)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating node reader")
	}
	ln := nr.leftCount()
	rn := nr.rightCount()

	if ln+rn <= 2 {
		return k, nil
	}

	if rn > weight*ln { // right is too big
		rnnr, err := newNodeReader(s, nr.rightChild())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node reader")
		}

		rln := rnnr.leftCount()
		rrn := rnnr.rightCount()

		if rln < rrn {
			return singleLeft(s, k)
		} else {
			return doubleLeft(s, k)
		}
	}

	if ln > weight*rn { // left is too big
		lnnr, err := newNodeReader(s, nr.leftChild())
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating node reader")
		}
		lln := lnnr.leftCount()
		lrn := lnnr.rightCount()

		if lrn < lln {
			return singleRight(s, k)
		} else {
			return doubleRight(s, k)
		}
	}

	return k, nil

}

func IsBalanced(s store.Store, root store.Address) (bool, error) {
	if root == store.NilAddress {
		return true, nil
	}

	nr, err := newNodeReader(s, root)
	if err != nil {
		return false, errors.Wrap(err, "while creating node reader")
	}

	lcnt := nr.leftCount()
	rcnt := nr.rightCount()

	if lcnt+rcnt <= 2 {
		return true, nil
	}

	if lcnt > weight*rcnt {
		return false, nil
	}

	lc := nr.leftChild()

	bal, err := IsBalanced(s, lc)
	if err != nil {
		return false, err
	}

	if !bal {
		return false, err
	}

	rc := nr.rightChild()

	return IsBalanced(s, rc)
}
