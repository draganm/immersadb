package btree_test

import (
	"testing"

	"github.com/draganm/immersadb/btree"
	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	ts, cleanup := btree.NewTestStore(t)
	defer cleanup()

	t.Run("when I create an empty btree", func(t *testing.T) {
		a, err := btree.CreateEmpty(ts)
		require.NoError(t, err)
		require.NotEqual(t, store.NilAddress, a)
		t.Run("then the count of the tree should be 0", func(t *testing.T) {
			cnt, err := btree.Count(ts, a)
			require.NoError(t, err)
			require.Equal(t, uint64(0), cnt)
		})

		t.Run("when I put one key/value into the empty btree", func(t *testing.T) {
			v1, err := data.StoreData(ts, []byte{3, 3, 3}, 256, 4)
			require.NoError(t, err)

			a, err = btree.Put(ts, a, []byte{1, 2, 3}, v1)
			require.NoError(t, err)

			t.Run("then the count of the tree should be 1", func(t *testing.T) {
				cnt, err := btree.Count(ts, a)
				require.NoError(t, err)
				require.Equal(t, uint64(1), cnt)
			})

			t.Run("then I should be able to find the key", func(t *testing.T) {
				va, err := btree.Get(ts, a, []byte{1, 2, 3})
				require.NoError(t, err)
				require.Equal(t, v1, va)
			})

		})
	})

}
