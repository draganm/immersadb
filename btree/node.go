package btree

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/draganm/immersadb/store"
)

type keyValue struct {
	Key   []byte
	Value store.Address
}

func (kv *keyValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%v: %s", kv.Key, kv.Value))
}

type node struct {
	Count    uint64
	m        int
	KVS      []keyValue
	Children []*node `json:",omitempty"`
}

type insertResult struct {
	didInsert bool
	didSplit  bool
	middle    keyValue
	left      *node
	right     *node
}

func (n *node) isFull() bool {
	return len(n.KVS) == 2*n.m+1
}

func (n *node) isLeaf() bool {
	return len(n.Children) == 0
}

func (n *node) insert(kv keyValue) insertResult {

	if n.isFull() {
		middleValue, left, right := n.split()
		var ir insertResult
		if bytes.Compare(kv.Key, middleValue.Key) < 0 {
			ir = left.insert(kv)
		} else {
			ir = right.insert(kv)
		}

		return insertResult{
			didInsert: ir.didInsert,
			didSplit:  true,
			left:      left,
			middle:    middleValue,
			right:     right,
		}

	}

	if !n.isLeaf() {
		idx := sort.Search(len(n.KVS), func(i int) bool {
			return bytes.Compare(n.KVS[i].Key, kv.Key) >= 0
		})
		if idx < len(n.KVS) && bytes.Compare(n.KVS[idx].Key, kv.Key) == 0 {
			n.KVS[idx] = kv
			return insertResult{}
		}

		ch := n.Children[idx]
		ir := ch.insert(kv)
		if ir.didInsert {
			n.Count++
		}
		if ir.didSplit {
			n.Children = append(n.Children[:idx], append([]*node{ir.left, ir.right}, n.Children[idx+1:]...)...)
			n.KVS = append(n.KVS[:idx], append([]keyValue{ir.middle}, n.KVS[idx:]...)...)
			return insertResult{
				didInsert: ir.didInsert,
			}
		}
		return ir

	}

	idx := sort.Search(len(n.KVS), func(i int) bool {
		return bytes.Compare(n.KVS[i].Key, kv.Key) >= 0
	})

	if idx < len(n.KVS) && bytes.Compare(n.KVS[idx].Key, kv.Key) == 0 {
		n.KVS[idx] = kv
		return insertResult{
			didInsert: false,
		}
	}

	n.KVS = append(n.KVS[:idx], append([]keyValue{kv}, n.KVS[idx:]...)...)
	n.Count++

	return insertResult{
		didInsert: true,
	}
}

func (n *node) split() (keyValue, *node, *node) {
	if !n.isFull() {
		// TODO: remove me
		panic("splitting a non-full node")
	}

	middleIdx := len(n.KVS) / 2

	left := &node{
		KVS:   n.KVS[:middleIdx],
		Count: n.Count / 2,
		m:     n.m,
	}

	right := &node{
		KVS:   n.KVS[middleIdx+1:],
		Count: n.Count / 2,
		m:     n.m,
	}

	if !n.isLeaf() {
		left.Children = n.Children[:middleIdx+1]
		right.Children = n.Children[middleIdx+1:]
	}

	return n.KVS[middleIdx], left, right

}

func insertIntoBtree(root *node, kv keyValue) *node {
	insertResult := root.insert(kv)
	if !insertResult.didSplit {
		return root
	}

	cnt := root.Count
	if insertResult.didInsert {
		cnt++
	}

	return &node{
		m:     root.m,
		Count: cnt,
		KVS:   []keyValue{insertResult.middle},
		Children: []*node{
			insertResult.left,
			insertResult.right,
		},
	}

}

func (n *node) toJSON() string {
	y, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		panic(err)
	}

	return string(y)
}
