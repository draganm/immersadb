package trie

// import (
// 	"fmt"

// 	"github.com/draganm/immersadb/store"
// )

// func PrintTrie(s store.Store, root store.Address, prefix string) error {
// 	f, err := s.Get(root)
// 	if err != nil {
// 		return err
// 	}

// 	fmt.Print(prefix)

// 	fmt.Printf("Key: %s", root)

// 	tm := NewTrieModifier(f)

// 	pr := tm.GetPrefix()
// 	fmt.Printf(" prefix: %x", pr)

// 	value := tm.GetChild(256)
// 	if value != store.NilAddress {
// 		fmt.Printf(" value: %s", value)
// 	} else {
// 		fmt.Print(" no value")
// 	}

// 	fmt.Println()

// 	for i := 0; i < 256; i++ {
// 		chk := tm.GetChild(i)
// 		if chk != store.NilAddress {
// 			PrintTrie(s, chk, fmt.Sprintf("%s  child %02x: ", prefix, i))
// 		}
// 	}

// 	return nil

// }
