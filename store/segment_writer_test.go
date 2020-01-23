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

	t.Run("segment children should have Nil Address", func(t *testing.T) {
		require.Equal(t, 3, sw.NumberOfChildren())
		require.Equal(t, store.NilAddress, sw.GetChildAddress(0))
		require.Equal(t, store.NilAddress, sw.GetChildAddress(1))
		require.Equal(t, store.NilAddress, sw.GetChildAddress(2))
	})

	t.Run("layer sizes should be set", func(t *testing.T) {
		require.Equal(t, uint64(4+1+4*8+1+8*3+3), sw.GetLayerTotalSize(0))
	})

	t.Run("total tree size should be set", func(t *testing.T) {
		require.Equal(t, uint64(4+1+4*8+1+8*3+3), sw.GetTotalTreeSize())
	})

	t.Run("when I change the data", func(t *testing.T) {
		copy(sw.Data, []byte{1, 2, 3})
		t.Run("then the data on the segment should be changed", func(t *testing.T) {
			require.Equal(t, sw.GetData(), []byte{1, 2, 3})
		})
	})

	t.Run("when I write another segment", func(t *testing.T) {
		sw2, err := store.NewSegmentWriter(st, 0, 1, 3)
		require.NoError(t, err)

		t.Run("it should have different address than the first segment", func(t *testing.T) {
			require.Equal(t, uint64(0x41), sw2.Address.Position())
		})

		t.Run("when I set previous segment as a child for this segment", func(t *testing.T) {
			sw2.SetChild(0, sw.Address)
			t.Run("it should modify the total layer size", func(t *testing.T) {
				require.Equal(t, uint64(0x72), sw2.SegmentReader.GetLayerTotalSize(0))
			})
			t.Run("it should the child address", func(t *testing.T) {
				require.Equal(t, store.Address(0x0), sw2.SegmentReader.GetChildAddress(0))
			})
		})
	})

}
