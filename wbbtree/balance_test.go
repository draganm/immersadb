package wbbtree_test

import (
	"bytes"
	"crypto/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func randomValue(t *testing.T, l int) []byte {
	rv := make([]byte, l)
	_, err := rand.Read(rv)
	require.NoError(t, err)
	return rv
}

func TestBalancingBug(t *testing.T) {

	kvs := []kv{
		kv{[]byte{0x2f}, []byte{0x2f}},
		kv{[]byte{0xa8}, []byte{0xa8}},
		kv{[]byte{0x09}, []byte{0x09}},
		kv{[]byte{0x05}, []byte{0x05}},
		kv{[]byte{0x32}, []byte{0x32}},
		kv{[]byte{0x99}, []byte{0x99}},
		kv{[]byte{0x69}, []byte{0x69}},
	}

	tt, cleanup := newTreeTester(t)
	defer cleanup()

	kvsorted := []kv{}

	for _, kv := range kvs {
		tt.insert(t, kv.key, kv.value)
		kvsorted = append(kvsorted, kv)
		sort.Slice(kvsorted, func(i, j int) bool {
			return bytes.Compare(kvsorted[i].key, kvsorted[j].key) < 0
		})

		require.Equal(t, kvsorted, tt.list(t))
	}

}

func TestTreeBalancing(t *testing.T) {

	t.Run("each step of inserting 250 random nodes to the tree produces a balanced tree", func(t *testing.T) {
		tt, cleanup := newTreeTester(t)
		defer cleanup()

		kvs := []kv{}

		for i := 0; i < 250; i++ {

			ln := 30

			var k []byte
			for k = randomValue(t, ln); tt.containsKey(t, k); k = randomValue(t, ln) {
			}

			v := randomValue(t, ln)

			tt.insert(t, k, v)

			kvs = append(kvs, kv{k, v})

			sort.Slice(kvs, func(i, j int) bool {
				return bytes.Compare(kvs[i].key, kvs[j].key) < 0
			})

			require.Equal(t, kvs, tt.list(t))

			tt.ensureBalanced(t)
		}

		t.Run("when I delete key one by one it should retain tree balance", func(t *testing.T) {
			for len(kvs) > 0 {
				first := kvs[0]
				kvs = kvs[1:]
				tt.delete(t, first.key)
				require.Equal(t, kvs, tt.list(t))
				tt.ensureBalanced(t)
			}
		})

	})
}
