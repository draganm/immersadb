package store_test

import (
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	td, cleanup := createTempDir(t)
	defer cleanup()

	st, err := store.Open(td)
	require.NoError(t, err)

	st, err = st.WithTransaction()
	require.NoError(t, err)

	t.Run("when I append a segment to l0", func(t *testing.T) {
		sw, err := st.CreateSegment(0, 0, 0, 0)
		require.NoError(t, err)

		t.Run("it should return an address in l0", func(t *testing.T) {
			require.Equal(t, 0, sw.Address.Segment())
		})

		t.Run("it should return a position in l0", func(t *testing.T) {
			require.Equal(t, uint64(0), sw.Address.Position())
		})

		t.Run("when I append another segment to l0", func(t *testing.T) {
			a2, err := st.CreateSegment(0, 0, 0, 0)
			require.NoError(t, err)

			t.Run("it should return an address in l0", func(t *testing.T) {
				require.Equal(t, 0, a2.Segment())
			})

			t.Run("it should return a position in l0", func(t *testing.T) {
				require.Equal(t, uint64(0x26), a2.Position())
			})
		})

	})

}
