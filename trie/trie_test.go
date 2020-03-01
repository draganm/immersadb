package trie

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

var valueAddress = store.NewAddress(0, 1)
var replacedValueAddress = store.NewAddress(0, 2)

func TestEmptyTrie(t *testing.T) {

	ts, cleanup := newTestStore(t)
	defer cleanup()

	tr := newEmptyTrie(ts)

	t.Run("when I insert a value into an empty trie", func(t *testing.T) {
		inserted, err := tr.insert([]byte{1, 2, 3}, valueAddress)
		require.NoError(t, err)

		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})
		t.Run("then the trie should contain the value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})

		t.Run("when I delete the value from the trie", func(t *testing.T) {
			err := tr.delete([]byte{1, 2, 3})
			require.NoError(t, err)
			t.Run("then the trie shold have count 0", func(t *testing.T) {
				assert.Equal(t, uint64(0), tr.count)
			})

			t.Run("then the trie should not contain the value anymore", func(t *testing.T) {
				_, err := tr.get([]byte{1, 2, 3})
				require.Error(t, err, ErrNotFound)
			})

		})
	})

}

func TestInsertLongerKey(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	tr := newEmptyTrie(ts)

	tr.insert([]byte{1, 2, 3}, valueAddress)

	t.Run("when I replace the value with the same key", func(t *testing.T) {
		inserted, err := tr.insert([]byte{1, 2, 3}, replacedValueAddress)
		require.NoError(t, err)

		t.Run("then the value should be replaced", func(t *testing.T) {
			require.False(t, inserted)
		})
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})
		t.Run("then the trie should contain the replaced value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})
	})

	t.Run("when I insert a with longer key and shared prefix", func(t *testing.T) {
		inserted, err := tr.insert([]byte{1, 2, 3, 4}, valueAddress)
		require.NoError(t, err)

		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
		t.Run("then the trie should have count 2", func(t *testing.T) {
			assert.Equal(t, uint64(2), tr.count)
		})
		t.Run("then the trie should contain the old value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})
		t.Run("then the trie should contain the new value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3, 4})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})

	})

}

func TestDeleteAndRemoveChild(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	tr := newEmptyTrie(ts)

	tr.insert([]byte{1, 2, 3}, valueAddress)
	tr.insert([]byte{1, 2, 3, 4}, replacedValueAddress)

	t.Run("when I delete value that is in the child", func(t *testing.T) {
		err := tr.delete([]byte{1, 2, 3, 4})
		require.NoError(t, err)
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})

		t.Run("then the trie should not have the child anymore", func(t *testing.T) {
			require.Nil(t, tr.children[4])
		})

		t.Run("then the trie should contain the not deleted value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})

	})

}

func TestDeleteAndCollapseParent(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	tr := newEmptyTrie(ts)

	tr.insert([]byte{1, 2, 3}, valueAddress)
	tr.insert([]byte{1, 2, 3, 4}, replacedValueAddress)

	t.Run("when I delete value that is in the parent", func(t *testing.T) {
		err := tr.delete([]byte{1, 2, 3})
		require.NoError(t, err)
		t.Run("then the trie should have count 1", func(t *testing.T) {
			assert.Equal(t, uint64(1), tr.count)
		})

		t.Run("then the trie should not have the long prefix", func(t *testing.T) {
			require.Equal(t, []byte{1, 2, 3, 4}, tr.prefix)
		})

		t.Run("then the trie should contain the not deleted value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3, 4})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})

	})

}

func TestInsertShorterKey(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	tr := newEmptyTrie(ts)

	tr.insert([]byte{1, 2, 3}, valueAddress)

	t.Run("when I insert a value with a shorter key and shared prefix", func(t *testing.T) {
		inserted, err := tr.insert([]byte{1, 2}, replacedValueAddress)
		require.NoError(t, err)

		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
		t.Run("then the trie should have count 2", func(t *testing.T) {
			assert.Equal(t, uint64(2), tr.count)
		})
		t.Run("then the trie should contain the old value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})
		t.Run("then the trie should contain the new value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})

	})

}

func TestBranchKey(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	tr := newEmptyTrie(ts)

	tr.insert([]byte{1, 2, 3}, valueAddress)

	t.Run("when I insert a value with a common prefix, branching off key", func(t *testing.T) {
		inserted, err := tr.insert([]byte{1, 2, 4}, replacedValueAddress)
		require.NoError(t, err)

		t.Run("then the value should be inserted", func(t *testing.T) {
			require.True(t, inserted)
		})
		t.Run("then the trie should have count 2", func(t *testing.T) {
			assert.Equal(t, uint64(2), tr.count)
		})
		t.Run("then the trie should contain the old value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueAddress, va)
		})
		t.Run("then the trie should contain the new value", func(t *testing.T) {
			va, err := tr.get([]byte{1, 2, 4})
			require.NoError(t, err)
			require.Equal(t, replacedValueAddress, va)
		})

	})

}

func TestRandomInsertDeleteGet(t *testing.T) {

	ts, cleanup := newTestStore(t)
	defer cleanup()

	tr := newEmptyTrie(ts)

	keys := [][]byte{}

	for i := 0; i < 200; i++ {

		key := []byte(nil)
		for {
			len := rand.Intn(50)
			key = make([]byte, len)
			_, err := rand.Read(key)
			require.NoError(t, err)
			_, err = tr.get(key)
			if err == ErrNotFound {
				break
			}
			require.NoError(t, err)
		}

		keys = append(keys, key)

		t.Run(fmt.Sprintf("when I insert key nr %d into the trie", i), func(t *testing.T) {
			inserted, err := tr.insert(key, valueAddress)
			require.NoError(t, err)

			t.Run("then the key should be inserted", func(t *testing.T) {
				require.True(t, inserted)
			})

			t.Run("then the trie could should be increased", func(t *testing.T) {
				require.Equal(t, uint64(i+1), tr.count)
			})

			t.Run("then the trie should contain the key", func(t *testing.T) {
				addr, err := tr.get(key)
				require.NoError(t, err)
				require.Equal(t, valueAddress, addr)
			})

		})

	}

	for i, key := range keys {
		t.Run(fmt.Sprintf("when I delete key %d from the trie", i), func(t *testing.T) {
			err := tr.delete(key)
			require.NoError(t, err)
			t.Run("then the count of elements in the trie should be reduced", func(t *testing.T) {
				require.Equal(t, uint64(len(keys)-i-1), tr.count)
			})

			t.Run("then I should not be able to find the key in the trie", func(t *testing.T) {
				_, err := tr.get(key)
				require.Error(t, err, ErrNotFound)
			})
		})
	}

}

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
