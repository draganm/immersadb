package trie_test

import (
	// "math/rand"
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/trie"
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestRandomInserts(t *testing.T) {
	t.Run("when I insert 100 random keys:values into a trie", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		root, err := trie.CreateEmpty(st)

		require.NoError(t, err)

		count := 200

		keys := [][]byte{}
		values := []store.Address{}

		var containsKey = func(k []byte) bool {
			for _, kk := range keys {
				if bytes.Equal(k, kk) {
					return true
				}
			}
			return false
		}

		for i := 0; i < count; i++ {

			var key []byte

			for containsKey(key) {
				keyLen := rand.Intn(50)

				key = make([]byte, keyLen)

				_, err = rand.Read(key)
				require.NoError(t, err)
			}

			keys = append(keys, key)

			valueLen := rand.Intn(200)
			value := make([]byte, valueLen)

			_, err = rand.Read(value)
			require.NoError(t, err)

			valueKey, err := data.StoreData(st, value, 8129, 4)
			require.NoError(t, err)
			values = append(values, valueKey)

			root, err = trie.Insert(st, root, key, valueKey)
			require.NoError(t, err)

			t.Run(fmt.Sprintf("it should store all keys and values up to #%d", i), func(t *testing.T) {
				for j := 0; j < i; j++ {
					k := keys[j]

					tv, err := trie.Get(st, root, k)
					require.NoError(t, err)
					require.Equal(t, values[j], tv, "Expected value stored under #%d to be equal", j)

				}
			})

		}

		t.Run("when I delete each key one by one", func(t *testing.T) {
			for len(keys) > 0 {
				key := keys[len(keys)-1]
				root, err = trie.Delete(st, root, key)
				require.NoError(t, err)
				keys = keys[:len(keys)-1]

				_, err = trie.Get(st, root, key)
				require.Equal(t, trie.ErrNotFound, errors.Cause(err))

			}
		})

	})
}
