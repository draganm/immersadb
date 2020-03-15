package btree_test

import (
	"testing"

	"github.com/draganm/immersadb/btree"
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
	})

}
