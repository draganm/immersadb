package trie_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

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
	})
}
