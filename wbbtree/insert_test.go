package wbbtree_test

import (
	"testing"

	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/store"
	"github.com/draganm/immersadb/wbbtree"
	"github.com/stretchr/testify/require"
)

func TestTreeWithOneElement(t *testing.T) {
	t.Run("when inserting element in an empty tree", func(t *testing.T) {
		st, cleanup := newTestStore(t)
		defer cleanup()

		valueKey, err := data.StoreData(st, []byte{1}, 8129, 4)
		require.NoError(t, err)

		nr, err := wbbtree.Insert(st, store.NilAddress, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		t.Run("it should contain the value", func(t *testing.T) {
			vk, err := wbbtree.Search(st, nr, []byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueKey, vk)
		})

		t.Run("when I replace the value", func(t *testing.T) {
			newValueKey, err := data.StoreData(st, []byte{2}, 8129, 4)
			require.NoError(t, err)

			nr, err = wbbtree.Insert(st, nr, []byte{1, 2, 3}, newValueKey)
			require.NoError(t, err)
			t.Run("it should replace the value", func(t *testing.T) {
				vk, err := wbbtree.Search(st, nr, []byte{1, 2, 3})
				require.NoError(t, err)
				require.Equal(t, newValueKey, vk)
			})
		})

		t.Run("it should have count of 1", func(t *testing.T) {
			cnt, err := wbbtree.Count(st, nr)
			require.NoError(t, err)
			require.Equal(t, uint64(1), cnt)
		})

		t.Run("when I delete the element", func(t *testing.T) {
			nr, err = wbbtree.Delete(st, nr, []byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, store.NilAddress, nr)
		})

		t.Run("it should have count of 0", func(t *testing.T) {
			cnt, err := wbbtree.Count(st, nr)
			require.NoError(t, err)
			require.Equal(t, uint64(0), cnt)
		})

	})
}

func TestTreeWithTwoElements(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	firstValueKey, err := data.StoreData(st, []byte{1}, 8129, 4)
	require.NoError(t, err)

	nr, err := wbbtree.Insert(st, store.NilAddress, []byte{1, 2, 3}, firstValueKey)
	require.NoError(t, err)

	secondValueKey, err := data.StoreData(st, []byte{2}, 8129, 4)
	require.NoError(t, err)

	t.Run("when inserting a value lower than previous", func(t *testing.T) {

		nr, err = wbbtree.Insert(st, nr, []byte{0, 2, 3}, secondValueKey)
		require.NoError(t, err)

		t.Run("it should contain the value", func(t *testing.T) {
			vk, err := wbbtree.Search(st, nr, []byte{0, 2, 3})
			require.NoError(t, err)
			require.Equal(t, secondValueKey, vk)
		})

		t.Run("it should contain the old value too", func(t *testing.T) {
			vk, err := wbbtree.Search(st, nr, []byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, firstValueKey, vk)
		})

	})

	t.Run("when inserting a value higher than previous", func(t *testing.T) {
		thirdValueKey, err := data.StoreData(st, []byte{2}, 8129, 4)
		require.NoError(t, err)

		nr, err = wbbtree.Insert(st, nr, []byte{2, 2, 3}, thirdValueKey)
		require.NoError(t, err)

		t.Run("it should contain the value", func(t *testing.T) {
			vk, err := wbbtree.Search(st, nr, []byte{2, 2, 3})
			require.NoError(t, err)
			require.Equal(t, thirdValueKey, vk)
		})

		t.Run("it should contain the old values too", func(t *testing.T) {
			vk, err := wbbtree.Search(st, nr, []byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, firstValueKey, vk)

			vk, err = wbbtree.Search(st, nr, []byte{0, 2, 3})
			require.NoError(t, err)
			require.Equal(t, secondValueKey, vk)
		})

	})
}
