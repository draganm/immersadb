package trie_test

import (
	"testing"

	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/trie"
	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestRepeatingSubtrie(t *testing.T) {

	st := fragment.NewStore(store.NewMemoryBackendFactory())
	valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
	require.NoError(t, err)

	k, err := trie.CreateEmpty(st)
	require.NoError(t, err)

	k, err = trie.Insert(st, k, []byte("0x102"), valueKey)
	require.NoError(t, err)

	k, err = trie.Insert(st, k, []byte("0x101"), valueKey)
	require.NoError(t, err)

	k, err = trie.Insert(st, k, []byte("0x100"), valueKey)
	require.NoError(t, err)

	keys := []string{}
	err = trie.ForEach(st, k, func(key []byte, value store.Address) error {
		keys = append(keys, string(key))
		return nil
	})

	require.Equal(t, []string{"0x100", "0x101", "0x102"}, keys)

	k, err = trie.Insert(st, k, []byte("0xff"), valueKey)
	require.NotEqual(t, store.NilAddress, k)

	keys = []string{}

	trie.ForEach(st, k, func(key []byte, value store.Address) error {
		keys = append(keys, string(key))
		return nil
	})

	require.Equal(t, []string{"0x100", "0x101", "0x102", "0xff"}, keys)

}
