package store_test

import (
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func createTestStore(t *testing.T) (store.Store, func() error) {
	td, cleanup := createTempDir(t)
	defer cleanup()

	l0, err := store.OpenOrCreateSegmentFile(filepath.Join(td, "l0"), 10*1024*1024)
	require.NoError(t, err)

	l1, err := store.OpenOrCreateSegmentFile(filepath.Join(td, "l1"), 10*1024*1024)
	require.NoError(t, err)

	return store.Store{l0, l1}, func() error {
		err := l0.Close()
		if err != nil {
			return err
		}
		err = l1.Close()
		if err != nil {
			return err
		}

		return cleanup()
	}
}
func TestSegmentWriter(t *testing.T) {
	st, cleanup := createTestStore(t)
	defer cleanup()

	sw, err := store.NewSegmentWriter(st, 0, 3, 3)
	require.NoError(t, err)

	sr, err := store.NewSegmentReader(st[0].MMap)
	require.NoError(t, err)

	t.Run("segment children should have Nil Address", func(t *testing.T) {
		require.Equal(t, 3, sr.NumberOfChildren())
		require.Equal(t, store.NilAddress, sr.GetChildAddress(0))
		require.Equal(t, store.NilAddress, sr.GetChildAddress(1))
		require.Equal(t, store.NilAddress, sr.GetChildAddress(2))
	})

	t.Run("layer sizes should be set", func(t *testing.T) {
		require.Equal(t, uint64(4+1+4*8+1+8*3+3), sr.GetLayerTotalSize(0))
	})

	t.Run("total tree size should be set", func(t *testing.T) {
		require.Equal(t, uint64(4+1+4*8+1+8*3+3), sr.GetTotalTreeSize())
	})

	t.Run("when I change the data", func(t *testing.T) {
		copy(sw.Data, []byte{1, 2, 3})
		t.Run("then the data on the segment should be changed", func(t *testing.T) {
			require.Equal(t, sr.GetData(), []byte{1, 2, 3})
		})
	})
}
