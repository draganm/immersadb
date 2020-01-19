package wbbtree

import (
	"github.com/draganm/immersadb/store"
)

const weight = 4

func balance(s store.Store, k store.Address) (store.Address, error) {
	if k == store.NilAddress {
		return k, nil

	}
	nr := newNodeReader(s, k)
	ln := nr.leftCount()
	rn := nr.rightCount()

	if nr.err() != nil {
		return store.NilAddress, nr.err()
	}

	if ln+rn <= 2 {
		return k, nil
	}

	if rn > weight*ln { // right is too big
		rnnr := newNodeReader(s, nr.rightChild())
		rln := rnnr.leftCount()
		rrn := rnnr.rightCount()

		if rnnr.err() != nil {
			return store.NilAddress, rnnr.err()
		}

		if rln < rrn {
			return singleLeft(s, k)
		} else {
			return doubleLeft(s, k)
		}
	}

	if ln > weight*rn { // left is too big
		lnnr := newNodeReader(s, nr.leftChild())
		lln := lnnr.leftCount()
		lrn := lnnr.rightCount()

		if lnnr.err() != nil {
			return store.NilAddress, lnnr.err()
		}

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

	nr := newNodeReader(s, root)

	lcnt := nr.leftCount()
	rcnt := nr.rightCount()

	if lcnt+rcnt <= 2 {
		return true, nil
	}

	if nr.err() != nil {
		return false, nr.err()
	}

	if lcnt > weight*rcnt {
		return false, nil
	}

	lc := nr.leftChild()
	if nr.err() != nil {
		return false, nr.err()
	}

	bal, err := IsBalanced(s, lc)
	if err != nil {
		return false, err
	}

	if !bal {
		return false, err
	}

	rc := nr.rightChild()

	if nr.err() != nil {
		return false, nr.err()
	}

	return IsBalanced(s, rc)
}
