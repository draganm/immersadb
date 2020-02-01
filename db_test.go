package immersadb_test

import (
	"io/ioutil"
	"os"
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

	var db *immersadb.DB

	t.Run("when I open database with an empty dir", func(t *testing.T) {
		var err error
		db, err = immersadb.Open(td)
		require.NoError(t, err)

		t.Run("when I start a new transaction", func(t *testing.T) {
			tx, err := db.Transaction()
			require.NoError(t, err)

			t.Run("when I create a map in root", func(t *testing.T) {
				err = tx.CreateMap("test")
				require.NoError(t, err)

				t.Run("then the root should have 1 element", func(t *testing.T) {
					cnt, err := tx.Count("")
					require.NoError(t, err)
					require.Equal(t, uint64(1), cnt)
				})

				t.Run("when I put data in the sub-map", func(t *testing.T) {
					err = tx.Put("test/abc", []byte{1, 2, 3})
					require.NoError(t, err)
					t.Run("then I should be able to read the data", func(t *testing.T) {
						d, err := tx.Get("test/abc")
						require.NoError(t, err)
						require.Equal(t, []byte{1, 2, 3}, d)
					})
				})
			})

			t.Run("when I commit the transaction", func(t *testing.T) {
				err = tx.Commit()
				require.NoError(t, err)
				t.Run("then the new map should be persisted", func(t *testing.T) {
					cnt, err := db.ReadTransaction().Count("")
					require.NoError(t, err)
					require.Equal(t, uint64(1), cnt)
				})
			})

		})
	})

	t.Run("when I reopen the database", func(t *testing.T) {
		err := db.Close()
		require.NoError(t, err)

		db, err = immersadb.Open(td)
		require.NoError(t, err)

		t.Run("then the new map should be persisted", func(t *testing.T) {
			cnt, err := db.ReadTransaction().Count("")
			require.NoError(t, err)
			require.Equal(t, uint64(1), cnt)
		})

	})
}
