package trie

import (
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestCreateEmpty(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	ad, err := CreateEmpty(ts)
	require.NoError(t, err)

	require.NotEqual(t, store.NilAddress, ad)
}

func TestPut(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	t.Run("when I create an empty trie", func(t *testing.T) {
		ad, err := CreateEmpty(ts)
		require.NoError(t, err)

		t.Run("and I put one element into it", func(t *testing.T) {
			ad, err = Put(ts, ad, []byte{1, 2, 3}, valueAddress)
			require.NoError(t, err)

			t.Run("then I should be able to retrieve that element", func(t *testing.T) {
				gad, err := Get(ts, ad, []byte{1, 2, 3})
				require.NoError(t, err)
				require.Equal(t, valueAddress, gad)
			})

		})
	})

}
