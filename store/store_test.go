package store_test

import (
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	td, cleanup := createTempDir(t)
	defer cleanup()

	l0, err := store.OpenOrCreateSegmentFile(filepath.Join(td, "l0"), 10*1024*1024)
	require.NoError(t, err)

	defer l0.Close()

	l1, err := store.OpenOrCreateSegmentFile(filepath.Join(td, "l1"), 10*1024*1024)
	require.NoError(t, err)

	defer l1.Close()

	st := store.Store{l0, l1}

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
