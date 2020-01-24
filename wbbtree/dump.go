package wbbtree

import (
	"fmt"

	"github.com/draganm/immersadb/store"
)

func Dump(s store.Store, root store.Address, prefix string) {
	if root == store.NilAddress {
		fmt.Println(prefix, "NIL")
		return
	}
	nr, err := newNodeReader(s, root)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%sKey: %x  LC: %d RC: %d Value %s\n", prefix, nr.key(), nr.leftCount(), nr.rightCount(), nr.value())
	Dump(s, nr.leftChild(), prefix+"L:  ")
	Dump(s, nr.rightChild(), prefix+"R:  ")
}
