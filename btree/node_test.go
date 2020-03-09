package btree

import (
	"encoding/json"
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestInsertIntoEmptyNode(t *testing.T) {

	n := &node{m: 3}

	t.Run("when I insert a key/value into an empty node", func(t *testing.T) {

		r := n.insert(keyValue{
			Key:   []byte{1, 2, 3},
			Value: store.NewAddress(0, 333),
		})

		require.True(t, r.DidInsert)

		t.Run("then the node should have the inserted keyValue", func(t *testing.T) {
			require.Len(t, n.KVS, 1)
			require.Equal(t, keyValue{
				Key:   []byte{1, 2, 3},
				Value: store.NewAddress(0, 333),
			}, n.KVS[0])
		})

		t.Run("then the node should have increased the count", func(t *testing.T) {
			require.Equal(t, uint64(1), n.Count)
		})

	})

}

func TestInsertLowerKeyIntoNodeWithOneKey(t *testing.T) {
	n := &node{m: 3}
	r := n.insert(keyValue{
		Key:   []byte{1, 2, 3},
		Value: store.NewAddress(0, 333),
	})
	require.True(t, r.DidInsert)

	t.Run("when I insert lower key/value", func(t *testing.T) {

		r := n.insert(keyValue{
			Key:   []byte{1, 0, 0},
			Value: store.NewAddress(0, 334),
		})
		require.True(t, r.DidInsert)

		t.Run("then the keyValue should be inserted before the old key/valye", func(t *testing.T) {
			require.Equal(t, []keyValue{
				keyValue{
					Key:   []byte{1, 0, 0},
					Value: store.NewAddress(0, 334),
				},
				keyValue{
					Key:   []byte{1, 2, 3},
					Value: store.NewAddress(0, 333),
				},
			}, n.KVS)
		})

		t.Run("then the node should have increased the count", func(t *testing.T) {
			require.Equal(t, uint64(2), n.Count)
		})

	})

}

func TestInsertHigherKeyIntoNodeWithOneKey(t *testing.T) {
	n := &node{m: 3}
	r := n.insert(keyValue{
		Key:   []byte{1, 2, 3},
		Value: store.NewAddress(0, 333),
	})
	require.True(t, r.DidInsert)

	t.Run("when I insert higher key/value", func(t *testing.T) {

		r := n.insert(keyValue{
			Key:   []byte{1, 2, 4},
			Value: store.NewAddress(0, 334),
		})
		require.True(t, r.DidInsert)

		t.Run("then the keyValue should be inserted after the old key/valye", func(t *testing.T) {
			require.Equal(t, []keyValue{
				keyValue{
					Key:   []byte{1, 2, 3},
					Value: store.NewAddress(0, 333),
				},
				keyValue{
					Key:   []byte{1, 2, 4},
					Value: store.NewAddress(0, 334),
				},
			}, n.KVS)
		})

		t.Run("then the node should have increased the count", func(t *testing.T) {
			require.Equal(t, uint64(2), n.Count)
		})

	})

}

func TestReplaceValueWithOneKey(t *testing.T) {
	n := &node{m: 3}
	r := n.insert(keyValue{
		Key:   []byte{1, 2, 3},
		Value: store.NewAddress(0, 333),
	})
	require.True(t, r.DidInsert)

	t.Run("when I insert higher key/value", func(t *testing.T) {

		r := n.insert(keyValue{
			Key:   []byte{1, 2, 3},
			Value: store.NewAddress(0, 334),
		})
		require.False(t, r.DidInsert)

		t.Run("then the keyValue should be inserted after the old key/valye", func(t *testing.T) {
			require.Equal(t, []keyValue{
				keyValue{
					Key:   []byte{1, 2, 3},
					Value: store.NewAddress(0, 334),
				},
			}, n.KVS)
		})

		t.Run("then the node should not increased the count", func(t *testing.T) {
			require.Equal(t, uint64(1), n.Count)
		})

	})

}

func TestSplittingTheChild(t *testing.T) {
	n := &node{m: 1}
	t.Run("when the child node is full", func(t *testing.T) {
		r := n.insert(keyValue{
			Key:   []byte{1, 2, 0},
			Value: store.NewAddress(0, 330),
		})
		require.True(t, r.DidInsert)
		r = n.insert(keyValue{
			Key:   []byte{1, 2, 1},
			Value: store.NewAddress(0, 331),
		})
		require.True(t, r.DidInsert)

		r = n.insert(keyValue{
			Key:   []byte{1, 2, 2},
			Value: store.NewAddress(0, 332),
		})
		require.True(t, r.DidInsert)

		t.Run("when I insert a new key/value", func(t *testing.T) {

			r = n.insert(keyValue{
				Key:   []byte{1, 2, 3},
				Value: store.NewAddress(0, 333),
			})
			require.True(t, r.DidInsert)

			t.Run("then the result should be a split node", func(t *testing.T) {
				require.True(t, r.DidSplit)
				requireJSONEqual(t, `
				  {
					"DidInsert": true,
					"DidSplit": true,
					"Middle": {
					  "Key": "AQIB",
					  "Value": 331
					},
					"Left": {
					  "Count": 1,
					  "KVS": [
						"[1 2 0]: Segment 0 Position 330"
					  ]
					},
					"Right": {
					  "Count": 2,
					  "KVS": [
						"[1 2 2]: Segment 0 Position 332",
						"[1 2 3]: Segment 0 Position 333"
					  ]
					}
				  }
				`, r.toJSON())
			})

		})

	})

}

func TestInsertingIntoNode(t *testing.T) {
	n := &node{m: 1}

	t.Run("when the root node is not a child", func(t *testing.T) {

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 0},
			Value: store.NewAddress(0, 330),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 1},
			Value: store.NewAddress(0, 331),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 2},
			Value: store.NewAddress(0, 332),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 3},
			Value: store.NewAddress(0, 333),
		})

		require.False(t, n.isLeaf())

		t.Run("when I insert another key/value", func(t *testing.T) {
			ir := n.insert(keyValue{
				Key:   []byte{1, 2, 4},
				Value: store.NewAddress(0, 334),
			})

			require.True(t, ir.DidInsert)
			require.False(t, ir.DidSplit)

			requireJSONEqual(t, `
			  {
				"Count": 5,
				"KVS": [
				  "[1 2 1]: Segment 0 Position 331"
				],
				"Children": [
				  {
					"Count": 1,
					"KVS": [
					  "[1 2 0]: Segment 0 Position 330"
					]
				  },
				  {
					"Count": 3,
					"KVS": [
					  "[1 2 2]: Segment 0 Position 332",
					  "[1 2 3]: Segment 0 Position 333",
					  "[1 2 4]: Segment 0 Position 334"
					]
				  }
				]
			  }
			`, n.toJSON(),
			)

		})

	})

}

func TestChangingValueInNode(t *testing.T) {
	n := &node{m: 1}

	t.Run("when the root node is not a child", func(t *testing.T) {

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 0},
			Value: store.NewAddress(0, 330),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 1},
			Value: store.NewAddress(0, 331),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 2},
			Value: store.NewAddress(0, 332),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 3},
			Value: store.NewAddress(0, 333),
		})

		require.False(t, n.isLeaf())

		t.Run("when I insert another key/value", func(t *testing.T) {
			ir := n.insert(keyValue{
				Key:   []byte{1, 2, 1},
				Value: store.NewAddress(0, 666),
			})

			require.False(t, ir.DidInsert)
			require.False(t, ir.DidSplit)

			requireJSONEqual(t, `
			  {
				"Count": 4,
				"KVS": [
				  "[1 2 1]: Segment 0 Position 666"
				],
				"Children": [
				  {
					"Count": 1,
					"KVS": [
					  "[1 2 0]: Segment 0 Position 330"
					]
				  },
				  {
					"Count": 2,
					"KVS": [
					  "[1 2 2]: Segment 0 Position 332",
					  "[1 2 3]: Segment 0 Position 333"
					]
				  }
				]
			  }
			`, n.toJSON(),
			)

		})

	})

}

func TestInsertingIntoNodeWithRightLeafSplitting(t *testing.T) {
	n := &node{m: 1}

	t.Run("when the root node is not a child and the leaf is full", func(t *testing.T) {

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 0},
			Value: store.NewAddress(0, 330),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 1},
			Value: store.NewAddress(0, 331),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 2},
			Value: store.NewAddress(0, 332),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 3},
			Value: store.NewAddress(0, 333),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 4},
			Value: store.NewAddress(0, 334),
		})

		t.Run("when I insert another key/value", func(t *testing.T) {
			ir := n.insert(keyValue{
				Key:   []byte{1, 2, 5},
				Value: store.NewAddress(0, 335),
			})

			require.True(t, ir.DidInsert)
			require.False(t, ir.DidSplit)

			requireJSONEqual(
				t,
				`{
					"Count": 6,
					"KVS": [
					  "[1 2 1]: Segment 0 Position 331",
					  "[1 2 3]: Segment 0 Position 333"
					],
					"Children": [
					  {
						"Count": 1,
						"KVS": [
						  "[1 2 0]: Segment 0 Position 330"
						]
					  },
					  {
						"Count": 1,
						"KVS": [
						  "[1 2 2]: Segment 0 Position 332"
						]
					  },
					  {
						"Count": 2,
						"KVS": [
						  "[1 2 4]: Segment 0 Position 334",
						  "[1 2 5]: Segment 0 Position 335"
						]
					  }
					]
				  }`,
				n.toJSON())

		})

	})

}

func TestInsertingIntoNodeWithLeftLeafSplitting(t *testing.T) {
	n := &node{m: 1}

	t.Run("when the root node is not a child and the leaf is full", func(t *testing.T) {

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 0},
			Value: store.NewAddress(0, 330),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 1},
			Value: store.NewAddress(0, 331),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 2},
			Value: store.NewAddress(0, 332),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 3},
			Value: store.NewAddress(0, 333),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 4},
			Value: store.NewAddress(0, 334),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 1, 2},
			Value: store.NewAddress(0, 322),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 1, 1},
			Value: store.NewAddress(0, 321),
		})

		t.Run("when I insert another key/value", func(t *testing.T) {
			ir := n.insert(keyValue{
				Key:   []byte{1, 1, 0},
				Value: store.NewAddress(0, 320),
			})

			require.True(t, ir.DidInsert)
			require.False(t, ir.DidSplit)

			requireJSONEqual(t, `
			{
				"Count": 8,
				"KVS": [
				  "[1 1 2]: Segment 0 Position 322",
				  "[1 2 1]: Segment 0 Position 331"
				],
				"Children": [
				  {
					"Count": 2,
					"KVS": [
					  "[1 1 0]: Segment 0 Position 320",
					  "[1 1 1]: Segment 0 Position 321"
					]
				  },
				  {
					"Count": 1,
					"KVS": [
					  "[1 2 0]: Segment 0 Position 330"
					]
				  },
				  {
					"Count": 3,
					"KVS": [
					  "[1 2 2]: Segment 0 Position 332",
					  "[1 2 3]: Segment 0 Position 333",
					  "[1 2 4]: Segment 0 Position 334"
					]
				  }
				]
			  }`,
				n.toJSON(),
			)

		})

	})

}

func TestInsertingIntoNodeWithNodeSplitting(t *testing.T) {
	n := &node{m: 1}

	t.Run("when the root node is full and the leaf is full", func(t *testing.T) {

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 0},
			Value: store.NewAddress(0, 330),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 1},
			Value: store.NewAddress(0, 331),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 2},
			Value: store.NewAddress(0, 332),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 3},
			Value: store.NewAddress(0, 333),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 4},
			Value: store.NewAddress(0, 334),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 1, 2},
			Value: store.NewAddress(0, 322),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 1, 1},
			Value: store.NewAddress(0, 321),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 1, 0},
			Value: store.NewAddress(0, 320),
		})

		n = insertIntoBtree(n, keyValue{
			Key:   []byte{1, 2, 5},
			Value: store.NewAddress(0, 335),
		})

		t.Run("when I insert another key/value", func(t *testing.T) {

			n = insertIntoBtree(n, keyValue{
				Key:   []byte{1, 2, 6},
				Value: store.NewAddress(0, 336),
			})

			requireJSONEqual(t, `
			  {
				"Count": 10,
				"KVS": [
				  "[1 2 1]: Segment 0 Position 331"
				],
				"Children": [
				  {
					"Count": 4,
					"KVS": [
					  "[1 1 2]: Segment 0 Position 322"
					],
					"Children": [
					  {
						"Count": 2,
						"KVS": [
						  "[1 1 0]: Segment 0 Position 320",
						  "[1 1 1]: Segment 0 Position 321"
						]
					  },
					  {
						"Count": 1,
						"KVS": [
						  "[1 2 0]: Segment 0 Position 330"
						]
					  }
					]
				  },
				  {
					"Count": 5,
					"KVS": [
					  "[1 2 3]: Segment 0 Position 333"
					],
					"Children": [
					  {
						"Count": 1,
						"KVS": [
						  "[1 2 2]: Segment 0 Position 332"
						]
					  },
					  {
						"Count": 3,
						"KVS": [
						  "[1 2 4]: Segment 0 Position 334",
						  "[1 2 5]: Segment 0 Position 335",
						  "[1 2 6]: Segment 0 Position 336"
						]
					  }
					]
				  }
				]
			  }`,
				n.toJSON(),
			)

		})

	})

}

func requireJSONEqual(t *testing.T, y1, y2 string) {
	var v1 interface{}
	var v2 interface{}

	err := json.Unmarshal([]byte(y1), &v1)
	require.NoError(t, err)

	err = json.Unmarshal([]byte(y2), &v2)
	require.NoError(t, err)

	y1b, err := json.MarshalIndent(v1, "", "  ")
	require.NoError(t, err)

	y2b, err := json.MarshalIndent(v2, "", "  ")
	require.NoError(t, err)

	require.Equal(t, string(y1b), string(y2b))
}
