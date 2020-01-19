package store_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func createTempDir(t *testing.T) (string, func() error) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return td, func() error {
		return os.RemoveAll(td)
	}
}

func TestSegmentFile(t *testing.T) {

	td, cleanup := createTempDir(t)
	defer cleanup()

	sfn := filepath.Join(td, "seg1")

	sf, err := store.OpenOrCreateSegmentFile(sfn, 10*1024*1024)
	require.NoError(t, err)

	defer sf.Close()

	t.Run("when the segment file is newly created", func(t *testing.T) {
		t.Run("then the WriteRegion should be empty", func(t *testing.T) {
			require.Equal(t, 0, len(sf.WriteRegion()))
		})
	})

	t.Run("when I ensure 100 bytes size", func(t *testing.T) {
		err = sf.EnsureSize(100)
		require.NoError(t, err)
		t.Run("then the segment file write region should be at least 100 bytes long", func(t *testing.T) {
			require.GreaterOrEqual(t, len(sf.WriteRegion()), 100)
		})
	})

	t.Run("when I write 3 bytes in the segment file", func(t *testing.T) {
		addr, err := sf.Write([]byte{1, 2, 3})
		require.NoError(t, err)

		t.Run("it should return address of those bytes", func(t *testing.T) {
			require.Equal(t, uint64(0), addr)
		})
	})

	t.Run("when I write 3 bytes in the segment file", func(t *testing.T) {
		addr, err := sf.Write([]byte{4, 5, 6})
		require.NoError(t, err)

		t.Run("it should return address of those bytes", func(t *testing.T) {
			require.Equal(t, uint64(3), addr)
		})
	})

	t.Run("I should be able to get the whole segment data", func(t *testing.T) {
		require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, []byte(sf.MMap)[:6])
	})

	t.Run("flushing data should not return an error", func(t *testing.T) {
		err = sf.Flush()
		require.NoError(t, err)
	})

	t.Run("when I close and re-open the segment file", func(t *testing.T) {
		err = sf.Close()
		require.NoError(t, err)

		sf, err = store.OpenOrCreateSegmentFile(sfn, 10*1024*1024)
		require.NoError(t, err)

	})

	t.Run("I should be able to get the whole segment data", func(t *testing.T) {
		require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, []byte(sf.MMap)[:6])
	})

}
