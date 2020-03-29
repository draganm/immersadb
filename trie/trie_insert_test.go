package trie_test

import (
	"testing"

	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/trie"
	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestCreateEmpty(t *testing.T) {
	st := fragment.NewStore(store.NewMemoryBackendFactory())
	k, err := trie.CreateEmpty(st)
	require.NoError(t, err)
	require.NotEqual(t, store.NilAddress, k)
}

func TestInsert(t *testing.T) {

	t.Run("inserting into an empty trie", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		trieKey, err := trie.Insert(st, root, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		require.NotEqual(t, store.NilAddress, trieKey)

		t.Run("should create a trie with one key/value", func(t *testing.T) {
			f, err := st.Get(trieKey)
			require.NoError(t, err)

			ch, err := f.Children()
			require.NoError(t, err)

			require.Equal(t, 257, ch.Len())

			valueKeyBytes, err := ch.At(256)
			require.NoError(t, err)

			require.Equal(t, valueKey, store.BytesToKey(valueKeyBytes))

		})

	})

	t.Run("inserting changing a value in a single value trie", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		trieKey, err := trie.Insert(st, root, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		require.NotEqual(t, store.NilAddress, trieKey)

		t.Run("when replacing the value for already set key", func(t *testing.T) {

			valueKey, err := data.StoreData(st, []byte{5, 4, 3}, 1024, 16)
			require.NoError(t, err)

			trieKey, err := trie.Insert(st, trieKey, []byte{1, 2, 3}, valueKey)
			require.NoError(t, err)

			t.Run("it should save the new value", func(t *testing.T) {
				f, err := st.Get(trieKey)
				require.NoError(t, err)

				ch, err := f.Children()
				require.NoError(t, err)

				require.Equal(t, 257, ch.Len())

				valueKeyBytes, err := ch.At(256)
				require.NoError(t, err)

				require.Equal(t, valueKey, store.BytesToKey(valueKeyBytes))

			})

		})

	})

	t.Run("inserting into a single value trie with common prefix", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		trieKey, err := trie.Insert(st, root, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		require.NotEqual(t, store.NilAddress, trieKey)

		t.Run("when inserting a new key with a shared prefix", func(t *testing.T) {

			secondValueKey, err := data.StoreData(st, []byte{5, 4, 3}, 1024, 16)
			require.NoError(t, err)

			trieKey, err := trie.Insert(st, trieKey, []byte{1, 2, 4}, secondValueKey)
			require.NoError(t, err)

			t.Run("it should save the new value", func(t *testing.T) {

				val, err := trie.Get(st, trieKey, []byte{1, 2, 3})
				require.NoError(t, err)
				require.Equal(t, valueKey, val)

			})

			t.Run("it should keep the old value", func(t *testing.T) {

				val, err := trie.Get(st, trieKey, []byte{1, 2, 4})
				require.NoError(t, err)
				require.Equal(t, secondValueKey, val)

			})

		})

	})

	t.Run("inserting into a single value trie with no common prefix", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		trieKey, err := trie.Insert(st, root, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		require.NotEqual(t, store.NilAddress, trieKey)

		t.Run("when inserting a new key with a shared prefix", func(t *testing.T) {

			secondValueKey, err := data.StoreData(st, []byte{5, 4, 3}, 1024, 16)
			require.NoError(t, err)

			trieKey, err := trie.Insert(st, trieKey, []byte{9, 10, 11}, secondValueKey)
			require.NoError(t, err)

			t.Run("it should save the new value", func(t *testing.T) {

				val, err := trie.Get(st, trieKey, []byte{9, 10, 11})
				require.NoError(t, err)
				require.Equal(t, secondValueKey, val)

			})

			t.Run("it should keep the old value", func(t *testing.T) {

				val, err := trie.Get(st, trieKey, []byte{1, 2, 3})
				require.NoError(t, err)
				require.Equal(t, valueKey, val)

			})

		})

	})

	t.Run("when trie has two key:values", func(t *testing.T) {

		store := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(store)
		require.NoError(t, err)

		valueKey1, err := data.StoreData(store, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(store, root, []byte{1, 2, 3}, valueKey1)
		require.NoError(t, err)

		valueKey2, err := data.StoreData(store, []byte{4, 5, 6}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(store, root, []byte{2, 3, 4}, valueKey2)
		require.NoError(t, err)

		t.Run("when I insert another key:value", func(t *testing.T) {

			valueKey3, err := data.StoreData(store, []byte{5, 6, 7}, 1024, 16)
			require.NoError(t, err)

			root, err = trie.Insert(store, root, []byte{3, 4, 5}, valueKey3)
			require.NoError(t, err)

		})

	})

	t.Run("when trie has two key:values", func(t *testing.T) {

		store := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(store)
		require.NoError(t, err)

		valueKey1, err := data.StoreData(store, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(store, root, []byte{1, 2, 3}, valueKey1)
		require.NoError(t, err)

		valueKey2, err := data.StoreData(store, []byte{4, 5, 6}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(store, root, []byte{2, 3, 4}, valueKey2)
		require.NoError(t, err)

		t.Run("when I insert an empty key", func(t *testing.T) {

			valueKey3, err := data.StoreData(store, []byte{5, 6, 7}, 1024, 16)
			require.NoError(t, err)

			root, err = trie.Insert(store, root, []byte{}, valueKey3)
			require.NoError(t, err)

			v, err := trie.Get(store, root, []byte{})
			require.NoError(t, err)

			require.Equal(t, valueKey3, v)

		})

	})
}
