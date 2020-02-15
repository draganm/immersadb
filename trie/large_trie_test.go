package trie_test

import (
	"encoding/binary"
	"testing"

	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/trie"
	"github.com/stretchr/testify/require"
)

func intToKey(i int) []byte {
	d := make([]byte, 8)
	binary.BigEndian.PutUint64(d, uint64(i))
	return d
}

func TestLargeTrie(t *testing.T) {
	ts, cleanup := newTestStore(t)
	defer cleanup()

	t.Run("when I have a persisted trie with 32 elements", func(t *testing.T) {
		tr := trie.NewEmpty(ts)
		da, err := data.StoreData(ts, []byte{1, 2, 3}, 1024, 2)
		require.NoError(t, err)

		for i := 0; i < 32; i++ {
			tr.Put([][]byte{intToKey(i)}, da, nil)
		}

		ta, err := tr.Persist()
		require.NoError(t, err)

		tr, err = trie.Load(ts, ta)
		require.NoError(t, err)

		require.Equal(t, uint64(32), tr.Count())

		t.Run("when I put another element into the trie", func(t *testing.T) {
			tr.Put([][]byte{intToKey(32)}, da, nil)

			t.Run("and I store and load the trie", func(t *testing.T) {
				ta, err := tr.Persist()
				require.NoError(t, err)
				tr, err = trie.Load(ts, ta)
				require.NoError(t, err)

				t.Run("then the loaded trie should have 33 elements", func(t *testing.T) {
					require.Equal(t, uint64(33), tr.Count())
				})
				t.Run("then the loaded trie should have all keys", func(t *testing.T) {
					for i := 0; i < 33; i++ {
						nda, err := tr.Get([][]byte{intToKey(i)})
						require.NoError(t, err)
						require.Equal(t, da, nda)
					}
				})

				t.Run("when I put 34th element into the trie", func(t *testing.T) {
					tr.Put([][]byte{intToKey(33)}, da, nil)
					t.Run("then the loaded trie should have 34 elements", func(t *testing.T) {
						require.Equal(t, uint64(34), tr.Count())
					})
					t.Run("then the loaded trie should have all keys", func(t *testing.T) {
						for i := 0; i < 34; i++ {
							nda, err := tr.Get([][]byte{intToKey(i)})
							require.NoError(t, err)
							require.Equal(t, da, nda)
						}
					})
					t.Run("when I store and load the trie", func(t *testing.T) {
						ta, err := tr.Persist()
						require.NoError(t, err)
						tr, err = trie.Load(ts, ta)
						require.NoError(t, err)
						t.Run("then the loaded trie should have 34 elements", func(t *testing.T) {
							require.Equal(t, uint64(34), tr.Count())
						})
						t.Run("then the loaded trie should have all keys", func(t *testing.T) {
							for i := 0; i < 34; i++ {
								nda, err := tr.Get([][]byte{intToKey(i)})
								require.NoError(t, err)
								require.Equal(t, da, nda)
							}
						})

					})

				})

				t.Run("when I put 7byte key element into the trie", func(t *testing.T) {
					tr.Put([][]byte{intToKey(0)[:7]}, da, nil)
					t.Run("then the trie should have 35 elements", func(t *testing.T) {
						require.Equal(t, uint64(35), tr.Count())
					})
					t.Run("then the trie should have all keys", func(t *testing.T) {
						for i := 0; i < 34; i++ {
							nda, err := tr.Get([][]byte{intToKey(i)})
							require.NoError(t, err)
							require.Equal(t, da, nda)
						}
						nda, err := tr.Get([][]byte{intToKey(0)[:7]})
						require.NoError(t, err)
						require.Equal(t, da, nda)
					})
					t.Run("when I store and load the trie", func(t *testing.T) {
						ta, err := tr.Persist()
						require.NoError(t, err)
						tr, err = trie.Load(ts, ta)
						require.NoError(t, err)

						t.Run("then the loaded trie should have 35 elements", func(t *testing.T) {
							require.Equal(t, uint64(35), tr.Count())
						})
						t.Run("then the loaded trie should have all keys", func(t *testing.T) {
							for i := 0; i < 34; i++ {
								nda, err := tr.Get([][]byte{intToKey(i)})
								require.NoError(t, err)
								require.Equal(t, da, nda)
							}
							nda, err := tr.Get([][]byte{intToKey(0)[:7]})
							require.NoError(t, err)
							require.Equal(t, da, nda)
						})

					})

				})

				t.Run("when I put 8byte key element with shorter prefix into the trie", func(t *testing.T) {
					tr.Put([][]byte{intToKey(256)}, da, nil)
					t.Run("then the trie should have 36 elements", func(t *testing.T) {
						require.Equal(t, uint64(36), tr.Count())
					})
					t.Run("then the trie should have all keys", func(t *testing.T) {
						for i := 0; i < 34; i++ {
							nda, err := tr.Get([][]byte{intToKey(i)})
							require.NoError(t, err)
							require.Equal(t, da, nda)
						}
						nda, err := tr.Get([][]byte{intToKey(256)})
						require.NoError(t, err)
						require.Equal(t, da, nda)
					})
					t.Run("when I store and load the trie", func(t *testing.T) {
						ta, err := tr.Persist()
						require.NoError(t, err)
						tr, err = trie.Load(ts, ta)
						require.NoError(t, err)

						t.Run("then the loaded trie should have 36 elements", func(t *testing.T) {
							require.Equal(t, uint64(36), tr.Count())
						})
						t.Run("then the loaded trie should have all keys", func(t *testing.T) {
							for i := 0; i < 34; i++ {
								nda, err := tr.Get([][]byte{intToKey(i)})
								require.NoError(t, err)
								require.Equal(t, da, nda)
							}
							nda, err := tr.Get([][]byte{intToKey(256)})
							require.NoError(t, err)
							require.Equal(t, da, nda)
						})

					})

				})

			})
		})

	})
}
