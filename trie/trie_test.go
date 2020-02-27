package trie

import (
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

var valueAddress = store.NewAddress(0, 1)
var replacedValueAddress = store.NewAddress(0, 2)

func TestEmptyTrie(t *testing.T) {
	tr := newEmptyTrie()

	t.Run("when I insert a value into an empty trie", func(t *testing.T) {
		inserted := tr.insert([]byte{1, 2, 3}, valueAddress)
		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
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

func TestInsertLongerKey(t *testing.T) {
	tr := newEmptyTrie()
	tr.insert([]byte{1, 2, 3}, valueAddress)

	t.Run("when I replace the value with the same key", func(t *testing.T) {
		inserted := tr.insert([]byte{1, 2, 3}, replacedValueAddress)
		t.Run("then the value should be replaced", func(t *testing.T) {
			require.False(t, inserted)
		})
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})
		t.Run("then the trie should contain the replaced value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})
	})

	t.Run("when I insert a with longer key and shared prefix", func(t *testing.T) {
		inserted := tr.insert([]byte{1, 2, 3, 4}, valueAddress)
		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
		t.Run("then the trie should have count 2", func(t *testing.T) {
			assert.Equal(t, uint64(2), tr.count)
		})
		t.Run("then the trie should contain the old value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})
		t.Run("then the trie should contain the new value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3, 4})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})

	})

}

func TestDeleteAndRemoveChild(t *testing.T) {
	tr := newEmptyTrie()
	tr.insert([]byte{1, 2, 3}, valueAddress)
	tr.insert([]byte{1, 2, 3, 4}, replacedValueAddress)

	t.Run("when I delete value that is in the child", func(t *testing.T) {
		err := tr.delete([]byte{1, 2, 3, 4})
		require.NoError(t, err)
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})

		t.Run("then the trie should not have the child anymore", func(t *testing.T) {
			require.Nil(t, tr.children[4])
		})

		t.Run("then the trie should contain the not deleted value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})

	})

}

func TestDeleteAndCollapseParent(t *testing.T) {
	tr := newEmptyTrie()
	tr.insert([]byte{1, 2, 3}, valueAddress)
	tr.insert([]byte{1, 2, 3, 4}, replacedValueAddress)

	t.Run("when I delete value that is in the parent", func(t *testing.T) {
		err := tr.delete([]byte{1, 2, 3})
		require.NoError(t, err)
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})

		t.Run("then the trie should not have the long prefix", func(t *testing.T) {
			require.Equal(t, []byte{1, 2, 3, 4}, tr.prefix)
		})

		t.Run("then the trie should contain the not deleted value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3, 4})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})

	})

}

func TestInsertShorterKey(t *testing.T) {
	tr := newEmptyTrie()
	tr.insert([]byte{1, 2, 3}, valueAddress)

	t.Run("when I insert a value with a shorter key and shared prefix", func(t *testing.T) {
		inserted := tr.insert([]byte{1, 2}, replacedValueAddress)
		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
		t.Run("then the trie should have count 2", func(t *testing.T) {
			assert.Equal(t, uint64(2), tr.count)
		})
		t.Run("then the trie should contain the old value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})
		t.Run("then the trie should contain the new value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})

	})

}

func TestBranchKey(t *testing.T) {
	tr := newEmptyTrie()
	tr.insert([]byte{1, 2, 3}, valueAddress)

	t.Run("when I insert a value with a common prefix, branching off key", func(t *testing.T) {
		inserted := tr.insert([]byte{1, 2, 4}, replacedValueAddress)
		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
		t.Run("then the trie should have count 2", func(t *testing.T) {
			assert.Equal(t, uint64(2), tr.count)
		})
		t.Run("then the trie should contain the old value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})
		t.Run("then the trie should contain the new value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 4})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})

	})

}
