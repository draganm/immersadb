package btree_test

import (
	"bytes"
	"math/rand"
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

func hasKey(keys [][]byte, key []byte) bool {
	for _, k := range keys {
		if bytes.Compare(k, key) == 0 {
			return true
		}
	}

	return false
}

func TestRandomInserts(t *testing.T) {

	ts, cleanup := btree.NewTestStore(t)
	defer cleanup()

	numberOfKeys := 2048

	keys := make([][]byte, numberOfKeys)
	values := make([]store.Address, numberOfKeys)

	for i := range keys {
		kl := 2 + rand.Intn(20)
		key := make([]byte, kl)

		n, err := rand.Read(key)
		require.NoError(t, err)
		require.Equal(t, n, kl)

		for hasKey(keys[:i], key) {
			n, err := rand.Read(key)
			require.NoError(t, err)
			require.Equal(t, n, kl)
		}

		keys[i] = key

		v, err := data.StoreData(ts, key, 256, 4)
		require.NoError(t, err)
		values[i] = v
	}

	a, err := btree.CreateEmpty(ts)
	require.NoError(t, err)

	for i, k := range keys {
		a, err = btree.Put(ts, a, k, values[i])
		require.NoError(t, err)
	}

	cnt, err := btree.Count(ts, a)
	require.NoError(t, err)
	require.Equal(t, uint64(numberOfKeys), cnt)

	for i, k := range keys {
		v, err := btree.Get(ts, a, k)
		require.NoError(t, err)
		require.Equal(t, values[i], v)
	}
}
