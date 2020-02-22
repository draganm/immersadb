package trie_test

import (
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/draganm/immersadb/trie"
	"github.com/stretchr/testify/require"
)

func TestNestedLargeTrie(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	t.Run("when I have a persisted trie with two elements that are empty tries", func(t *testing.T) {
		tr := trie.NewEmpty(ts)

		tr.Put([][]byte{[]byte("abc")}, store.NilAddress, trie.NewEmpty(ts))
		tr.Put([][]byte{[]byte("def")}, store.NilAddress, trie.NewEmpty(ts))

		ta, err := tr.Persist()
		require.NoError(t, err)
		tr, err = trie.Load(ts, ta)
		require.NoError(t, err)

		require.Equal(t, uint64(2), tr.Count())
		t.Run("when I create another empty trie in the first sub-trie", func(t *testing.T) {
			err = tr.Put([][]byte{[]byte("abc"), []byte("123")}, store.NilAddress, trie.NewEmpty(ts))
			require.NoError(t, err)
			t.Run("when I store and reload the trie", func(t *testing.T) {
				addr, err := tr.Persist()
				require.NoError(t, err)
				tr, err = trie.Load(ts, addr)
				require.NoError(t, err)
			})
		})

	})
}
