package btree

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
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

	store   store.Store
	address store.Address
}

type insertResult struct {
	DidInsert bool
	DidSplit  bool
	Middle    keyValue
	Left      *node
	Right     *node
}

func (i insertResult) toJSON() string {
	y, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		panic(err)
	}

	return string(y)
}

func (n *node) isFull() bool {
	return len(n.KVS) == 2*n.m+1
}

func (n *node) isLeaf() bool {
	return len(n.Children) == 0
}

func (n *node) load() error {
	if n.address == store.NilAddress {
		return nil
	}

	sr := n.store.GetSegment(n.address)

	d := sr.GetData()

	if len(d) < 8 {
		return errors.New("segment is too short")
	}

	count := binary.BigEndian.Uint64(d)

	d = d[8:]

	n.Count = count

	kvs := []keyValue{}

	for len(d) > 0 {
		if len(d) < 2 {
			return errors.New("segment data key length must be at least 2 bytes")
		}

		kl := int(binary.BigEndian.Uint16(d))

		d = d[2:]
		if len(d) < kl {
			return errors.New("key length is larger than available data")
		}

		k := make([]byte, kl)
		copy(k, d)

		d = d[kl:]
		kvs = append(kvs, keyValue{Key: k})

	}

	if sr.NumberOfChildren() < len(kvs) {
		return errors.New("segment doesn't have enough children for all values")
	}

	for i := range kvs {
		kvs[i].Value = sr.GetChildAddress(i)
	}

	n.KVS = kvs

	if sr.NumberOfChildren() == len(kvs) {
		n.address = store.NilAddress
		return nil
	}

	if sr.NumberOfChildren() != len(kvs)*2+1 {
		return errors.New("segment doesn't have enough children for child addresses")
	}

	children := []*node{}

	for i := 0; i < len(kvs)+1; i++ {
		children = append(children, &node{
			m:       n.m,
			store:   n.store,
			address: sr.GetChildAddress(len(kvs) + i),
		})
	}

	n.Children = children
	n.address = store.NilAddress

	return nil
}

func (n *node) persist() (store.Address, error) {
	if n.address != store.NilAddress {
		return n.address, nil
	}

	dataSize := 8

	for _, kv := range n.KVS {
		dataSize += 2 + len(kv.Key)
	}

	noc := len(n.KVS) + len(n.Children)

	sw, err := n.store.CreateSegment(0, store.TypeBTreeNode, noc, dataSize)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating a new segment")
	}

	d := sw.Data

	binary.BigEndian.PutUint64(d, n.Count)
	d = d[8:]

	for _, kv := range n.KVS {
		binary.BigEndian.PutUint16(d, uint16(len(kv.Key)))
		d = d[2:]
		copy(d, kv.Key)
		d = d[len(kv.Key):]
	}

	for i, kv := range n.KVS {
		sw.SetChild(i, kv.Value)
	}

	for i, c := range n.Children {
		addr, err := c.persist()
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while persisting a child")
		}
		sw.SetChild(len(n.KVS)+i, addr)
	}

	n.address = sw.Address

	return n.address, nil
}

func (n *node) insert(kv keyValue) (ir insertResult, err error) {

	err = n.load()
	if err != nil {
		return insertResult{}, err
	}

	if n.isFull() {
		middleValue, left, right := n.split()
		if bytes.Compare(kv.Key, middleValue.Key) < 0 {
			ir, err = left.insert(kv)
			if err != nil {
				return insertResult{}, err
			}
		} else {
			ir, err = right.insert(kv)
			if err != nil {
				return insertResult{}, err
			}
		}

		return insertResult{
			DidInsert: ir.DidInsert,
			DidSplit:  true,
			Left:      left,
			Middle:    middleValue,
			Right:     right,
		}, nil

	}

	if !n.isLeaf() {
		idx := sort.Search(len(n.KVS), func(i int) bool {
			return bytes.Compare(n.KVS[i].Key, kv.Key) >= 0
		})
		if idx < len(n.KVS) && bytes.Compare(n.KVS[idx].Key, kv.Key) == 0 {
			n.KVS[idx] = kv
			return insertResult{}, nil
		}

		ch := n.Children[idx]
		ir, err := ch.insert(kv)
		if err != nil {
			return insertResult{}, err
		}

		if ir.DidInsert {
			n.Count++
		}
		if ir.DidSplit {
			n.Children = append(n.Children[:idx], append([]*node{ir.Left, ir.Right}, n.Children[idx+1:]...)...)
			n.KVS = append(n.KVS[:idx], append([]keyValue{ir.Middle}, n.KVS[idx:]...)...)
			return insertResult{
				DidInsert: ir.DidInsert,
			}, nil
		}
		return ir, nil

	}

	idx := sort.Search(len(n.KVS), func(i int) bool {
		return bytes.Compare(n.KVS[i].Key, kv.Key) >= 0
	})

	if idx < len(n.KVS) && bytes.Compare(n.KVS[idx].Key, kv.Key) == 0 {
		n.KVS[idx] = kv
		return insertResult{
			DidInsert: false,
		}, nil
	}

	n.KVS = append(n.KVS[:idx], append([]keyValue{kv}, n.KVS[idx:]...)...)
	n.Count++

	return insertResult{
		DidInsert: true,
	}, nil
}

func (n *node) split() (keyValue, *node, *node) {
	if !n.isFull() {
		// TODO: remove me
		panic("splitting a non-full node")
	}

	middleIdx := len(n.KVS) / 2

	left := &node{
		KVS:     n.KVS[:middleIdx],
		Count:   n.Count / 2,
		m:       n.m,
		store:   n.store,
		address: store.NilAddress,
	}

	right := &node{
		KVS:     n.KVS[middleIdx+1:],
		Count:   n.Count / 2,
		m:       n.m,
		store:   n.store,
		address: store.NilAddress,
	}

	if !n.isLeaf() {
		left.Children = n.Children[:middleIdx+1]
		right.Children = n.Children[middleIdx+1:]
	}

	return n.KVS[middleIdx], left, right

}

func insertIntoBtree(root *node, kv keyValue) (*node, error) {
	insertResult, err := root.insert(kv)
	if err != nil {
		return nil, err
	}
	if !insertResult.DidSplit {
		return root, nil
	}

	cnt := root.Count
	if insertResult.DidInsert {
		cnt++
	}

	return &node{
		m:     root.m,
		Count: cnt,
		KVS:   []keyValue{insertResult.Middle},
		Children: []*node{
			insertResult.Left,
			insertResult.Right,
		},
		store:   root.store,
		address: store.NilAddress,
	}, nil

}

func (n *node) toJSON() string {
	y, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		panic(err)
	}

	return string(y)
}
