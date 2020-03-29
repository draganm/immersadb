package trie_test

import (
	"testing"

	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/trie"
	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestForAfterEach(t *testing.T) {
	t.Run("empty trie never calls the iterator", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		called := false

		trie.ForEachAfter(st, root, nil, func(key []byte, value store.Address) error {
			called = true
			return nil
		})

		require.False(t, called)

	})

	t.Run("when trie has one key:value", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(st, root, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		t.Run("it should call the iterator function with the key:value", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			trie.ForEachAfter(st, root, nil, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return nil
			})

			require.Equal(t, 1, len(keys))
			require.Equal(t, 1, len(values))
			require.Equal(t, []byte{1, 2, 3}, keys[0])

		})

	})

	t.Run("when trie has two key:values", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(st, root, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		valueKey2, err := data.StoreData(st, []byte{4, 5, 6}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(st, root, []byte{1, 2, 4}, valueKey2)
		require.NoError(t, err)

		t.Run("when I iterate after empty", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, nil, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return nil
			})
			require.NoError(t, err)

			t.Run("it should call the iterator function with the both key:values", func(t *testing.T) {
				require.Equal(t, 2, len(keys))
				require.Equal(t, 2, len(values))
				require.Equal(t, []byte{1, 2, 3}, keys[0])
				require.Equal(t, []byte{1, 2, 4}, keys[1])
				require.Equal(t, valueKey, values[0])
				require.Equal(t, valueKey2, values[1])
			})

		})

		t.Run("when I iterate after the first", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, []byte{1, 2, 3}, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return nil
			})
			require.NoError(t, err)

			t.Run("it should call the iterator function with the second key:value", func(t *testing.T) {
				require.Equal(t, 1, len(keys))
				require.Equal(t, 1, len(values))
				require.Equal(t, []byte{1, 2, 4}, keys[0])
				require.Equal(t, valueKey2, values[0])
			})

		})

		t.Run("when I iterate after the second", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, []byte{1, 2, 4}, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return nil
			})
			require.NoError(t, err)

			t.Run("it should not call the iterator function", func(t *testing.T) {
				require.Equal(t, 0, len(keys))
				require.Equal(t, 0, len(values))
			})

		})

		t.Run("when I stop the iteration after the first key:value", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, nil, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return trie.StopIteration
			})
			require.NoError(t, err)

			t.Run("it should not call the iterator function the second time", func(t *testing.T) {
				require.Equal(t, 1, len(keys))
				require.Equal(t, 1, len(values))
				require.Equal(t, []byte{1, 2, 3}, keys[0])
				require.Equal(t, valueKey, values[0])

			})

		})
	})

	t.Run("when trie has three key:values", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)
		require.NoError(t, err)

		valueKey, err := data.StoreData(st, []byte{3, 4, 5}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(st, root, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		valueKey2, err := data.StoreData(st, []byte{4, 5, 6}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(st, root, []byte{1, 2, 4}, valueKey2)
		require.NoError(t, err)

		valueKey3, err := data.StoreData(st, []byte{5, 6, 7}, 1024, 16)
		require.NoError(t, err)

		root, err = trie.Insert(st, root, []byte{1, 2, 3, 4}, valueKey3)
		require.NoError(t, err)

		t.Run("when I iterate after empty", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, nil, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return nil
			})
			require.NoError(t, err)

			t.Run("it should call the iterator function with the both key:values", func(t *testing.T) {
				require.Equal(t, 3, len(keys))
				require.Equal(t, 3, len(values))

				require.Equal(t, []byte{1, 2, 3}, keys[0])
				require.Equal(t, []byte{1, 2, 3, 4}, keys[1])
				require.Equal(t, []byte{1, 2, 4}, keys[2])
				require.Equal(t, valueKey, values[0])
				require.Equal(t, valueKey3, values[1])
				require.Equal(t, valueKey2, values[2])
			})

		})

		t.Run("when I iterate after the first", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, []byte{1, 2, 3}, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return nil
			})
			require.NoError(t, err)

			t.Run("it should call the iterator function with the second and third key:value", func(t *testing.T) {
				require.Equal(t, [][]byte(
					[][]byte{
						[]byte{0x1, 0x2, 0x3, 0x4},
						[]byte{0x1, 0x2, 0x4},
					},
				), keys)
			})

		})

		t.Run("when I iterate after the second", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, []byte{1, 2, 3, 4}, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return nil
			})
			require.NoError(t, err)

			t.Run("it should not call the iterator function", func(t *testing.T) {
				require.Equal(t, [][]byte(
					[][]byte{
						[]byte{0x1, 0x2, 0x4},
					},
				), keys)
			})

		})

		t.Run("when I stop the iteration after the first key:value", func(t *testing.T) {

			var keys [][]byte
			var values []store.Address

			err = trie.ForEachAfter(st, root, nil, func(key []byte, value store.Address) error {
				keys = append(keys, key)
				values = append(values, value)
				return trie.StopIteration
			})
			require.NoError(t, err)

			t.Run("it should not call the iterator function the second time", func(t *testing.T) {
				require.Equal(t, 1, len(keys))
				require.Equal(t, 1, len(values))
				require.Equal(t, []byte{1, 2, 3}, keys[0])
				require.Equal(t, valueKey, values[0])

			})

		})
	})

}
