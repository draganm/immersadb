package immersadb_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb"
	"github.com/stretchr/testify/require"
)

func createTempDir(t *testing.T) (string, func() error) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return td, func() error {
		return os.RemoveAll(td)
	}
}
func TestDatabaseCreation(t *testing.T) {
	td, cleanup := createTempDir(t)
	defer cleanup()
	t.Run("when I open database with an empty dir", func(t *testing.T) {
		_, err := immersadb.Open(td)
		require.NoError(t, err)
		t.Run("It should create layer files and root", func(t *testing.T) {
			rs, err := os.Stat(filepath.Join(td, "root"))
			require.NoError(t, err)
			require.Equal(t, int64(8), rs.Size())

			l1s, err := os.Stat(filepath.Join(td, "layer-1"))
			require.NoError(t, err)
			require.Equal(t, int64(1024*1024), l1s.Size())

			_, err = os.Stat(filepath.Join(td, "layer-2"))
			require.NoError(t, err)
			_, err = os.Stat(filepath.Join(td, "layer-3"))
			require.NoError(t, err)

		})
	})
}
