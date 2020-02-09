package trie_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/store"
	"github.com/draganm/immersadb/trie"
	"github.com/stretchr/testify/require"
)

func createTempDir(t *testing.T) (string, func() error) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return td, func() error {
		return os.RemoveAll(td)
	}
}

func newTestStore(t *testing.T) (store.Store, func() error) {
	td, cleanup := createTempDir(t)

	l0, err := store.OpenOrCreateSegmentFile(filepath.Join(td, "l0"), 10*1024*1024)
	require.NoError(t, err)

	st := store.Store{l0}

	return st, func() error {
		err = l0.Close()
		if err != nil {
			return err
		}
		return cleanup()
	}
}

func TestLoadingAndStoring(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	t.Run("when I persist an empty trie", func(t *testing.T) {
		empty := trie.NewEmpty(ts)
		ad, err := empty.Persist()
		require.NoError(t, err)
		require.NotEqual(t, store.NilAddress, ad)
		t.Run("then I should be able to load the empty trie", func(t *testing.T) {
			loadedEmpty := trie.Load(ts, ad)
			require.Equal(t, uint64(0), loadedEmpty.Count())
		})

		t.Run("when I put a key/value to the empty trie", func(t *testing.T) {
			da, err := data.StoreData(ts, []byte{1, 2, 3}, 1024, 2)
			require.NoError(t, err)
			empty.Put([][]byte{[]byte{1, 2, 3}}, da)

			t.Run("then I the cound should be 1", func(t *testing.T) {
				require.Equal(t, uint64(1), empty.Count())
			})

			t.Run("then I should be able to get the stored value", func(t *testing.T) {
				v, err := empty.Get([][]byte{[]byte{1, 2, 3}})
				require.NoError(t, err)
				require.Equal(t, da, v)
			})

			t.Run("and I persist the trie", func(t *testing.T) {
				pa, err := empty.Persist()
				require.NoError(t, err)

				t.Run("when I load the trie node", func(t *testing.T) {
					leaf := trie.Load(ts, pa)
					require.NoError(t, err)
					t.Run("then I should be able to get the stored value", func(t *testing.T) {
						v, err := leaf.Get([][]byte{[]byte{1, 2, 3}})
						require.NoError(t, err)
						require.Equal(t, da, v)
					})
					t.Run("then I the cound should be 1", func(t *testing.T) {
						require.Equal(t, uint64(1), leaf.Count())
					})
				})

			})
		})
	})
}
