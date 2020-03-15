package btree

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb/data"
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

func NewTestStore(t *testing.T) (store.Store, func() error) {
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

func TestPersistingAndLoadingLeaf(t *testing.T) {
	ts, cleanup := NewTestStore(t)
	defer cleanup()

	v1, err := data.StoreData(ts, []byte{3, 3, 3}, 256, 4)
	require.NoError(t, err)

	v2, err := data.StoreData(ts, []byte{3, 3, 4}, 256, 4)
	require.NoError(t, err)

	n := &node{
		Count: 2,
		m:     1,
		KVS: []keyValue{
			{Key: []byte{1, 2, 3}, Value: v1},
			{Key: []byte{1, 2, 4}, Value: v2},
		},
		store:   ts,
		address: store.NilAddress,
	}

	t.Run("when I store the node", func(t *testing.T) {
		addr, err := n.persist()
		require.NoError(t, err)

		require.Equal(t, addr, n.address)

		t.Run("when I load the node", func(t *testing.T) {
			err = n.load()
			require.NoError(t, err)

			t.Run("then the node should be the same", func(t *testing.T) {
				requireJSONEqual(
					t,
					`
					  {
						"Count": 2,
						"KVS": [
						  "[1 2 3]: Segment 0 Position 0",
						  "[1 2 4]: Segment 0 Position 41"
						]
					  }`,
					n.toJSON(),
				)
			})
		})
	})
}

func TestPersistingAndLoadingNode(t *testing.T) {
	ts, cleanup := NewTestStore(t)
	defer cleanup()

	v1, err := data.StoreData(ts, []byte{3, 3, 3}, 256, 4)
	require.NoError(t, err)

	v2, err := data.StoreData(ts, []byte{3, 3, 4}, 256, 4)
	require.NoError(t, err)

	v3, err := data.StoreData(ts, []byte{3, 3, 5}, 256, 4)
	require.NoError(t, err)

	n := &node{
		Count: 3,
		m:     1,
		KVS: []keyValue{
			{Key: []byte{1, 2, 4}, Value: v2},
		},
		store:   ts,
		address: store.NilAddress,
		Children: []*node{
			{
				Count: 1,
				m:     1,
				KVS: []keyValue{
					{Key: []byte{1, 2, 3}, Value: v1},
				},
				store:   ts,
				address: store.NilAddress,
			},
			{
				Count: 1,
				m:     1,
				KVS: []keyValue{
					{Key: []byte{1, 2, 5}, Value: v3},
				},
				store:   ts,
				address: store.NilAddress,
			},
		},
	}

	t.Run("when I store the node", func(t *testing.T) {
		addr, err := n.persist()
		require.NoError(t, err)

		require.Equal(t, addr, n.address)

		t.Run("when I load the node", func(t *testing.T) {
			err = n.load()
			require.NoError(t, err)

			for _, c := range n.Children {
				err = c.load()
				require.NoError(t, err)
			}

			t.Run("then the node should be the same", func(t *testing.T) {
				fmt.Println(n.toJSON())
				requireJSONEqual(
					t,
					`
					  {
						"Count": 3,
						"KVS": [
						  "[1 2 4]: Segment 0 Position 41"
						],
						"Children": [
						  {
							"Count": 1,
							"KVS": [
							  "[1 2 3]: Segment 0 Position 0"
							]
						  },
						  {
							"Count": 1,
							"KVS": [
							  "[1 2 5]: Segment 0 Position 82"
							]
						  }
						]
					  }`,
					n.toJSON(),
				)

				require.Equal(t, store.NilAddress, n.address)
				for _, c := range n.Children {
					require.Equal(t, store.NilAddress, c.address)
				}
			})
		})
	})
}
