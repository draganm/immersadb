package trie

import (
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

var valueAddress = store.NewAddress(0, 0)

func TestEmptyTrie(t *testing.T) {
	tr := newEmptyTrie()

	t.Run("when I insert a value into an empty trie", func(t *testing.T) {
		tr.insert([]byte{1, 2, 3}, valueAddress)
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})
		t.Run("then the trie should contain the value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})

		t.Run("when I delete the value from the trie", func(t *testing.T) {
			err := tr.delete([]byte{1, 2, 3})
			require.NoError(t, err)
			t.Run("then the trie shold have count 0", func(t *testing.T) {
				assert.Equal(t, uint64(0), tr.count)
			})

			t.Run("then the trie should not contain the value anymore", func(t *testing.T) {
				_, err := tr.get([]byte{1, 2, 3})
				require.Error(t, err, ErrNotFound)
			})

		})
	})

}
