package ttfmap

import (
	"errors"
	"fmt"
	"sort"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

var ErrNotTTFMapChunk = errors.New("Not a 2-3-4 Map leaf chunk")

var ErrKeyNotFound = errors.New("Key not found")

type KV struct {
	Key   string
	Value uint64
}

type LazyNode struct {
	parent   *LazyNode
	store    store.Store
	addr     uint64
	dirty    bool
	loaded   bool
	values   []*KV
	children []*LazyNode
}

func NewLazyNode(s store.Store, addr uint64) *LazyNode {
	return &LazyNode{
		store: s,
		addr:  addr,
	}
}

func (n *LazyNode) AddChild(values map[string]uint64) (*LazyNode, error) {
	n.load()
	keys := []string{}
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := []*KV{}

	for _, k := range keys {
		vals = append(vals, &KV{k, values[k]})
	}

	child := &LazyNode{
		store:  n.store,
		loaded: true,
		dirty:  true,
		values: vals,
	}
	n.dirty = true
	child.parent = n
	n.children = append(n.children, child)
	return child, nil
}

func NewInMemoryLazyRootNode(s store.Store, values map[string]uint64) *LazyNode {
	keys := []string{}
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := []*KV{}

	for _, k := range keys {
		vals = append(vals, &KV{k, values[k]})
	}

	n := &LazyNode{
		store:  s,
		loaded: true,
		dirty:  true,
		values: vals,
	}
	return n
}

func (n *LazyNode) load() {
	if n.dirty {
		return
	}
	if n.loaded {
		return
	}
	t, refs, data := chunk.Parts(n.store.Chunk(n.addr))

	if t != chunk.TTFMapNode {
		// should never occur
		panic(ErrNotTTFMapChunk)
	}

	var keys []string
	err := msgpack.Unmarshal(data, &keys)
	if err != nil {
		// should never occur
		panic(err)
	}

	n.values = make([]*KV, len(keys))

	for i, k := range keys {
		n.values[i] = &KV{k, refs[i]}
	}

	numberOfChildren := len(refs) - len(keys)

	n.children = make([]*LazyNode, numberOfChildren)

	for i, caddr := range refs[len(keys):] {
		n.children[i] = NewLazyNode(n.store, caddr)
		n.children[i].parent = n
	}

	n.loaded = true

	return

}

func (n *LazyNode) isLeaf() bool {
	n.load()
	return len(n.children) == 0
}

func (n *LazyNode) isFourNode() bool {
	return len(n.values) == 3
}

func (n *LazyNode) String() string {
	keys := []string{}
	for _, kv := range n.values {
		keys = append(keys, kv.Key)
	}

	return fmt.Sprintf("[Vals: %s, Children: %s]", keys, n.children)
}

func (n *LazyNode) Insert(key string, valueAddr uint64) *LazyNode {
	n.load()
	for _, kv := range n.values {
		if kv.Key == key {
			kv.Value = valueAddr
			n.dirty = true
			return nil
		}
	}

	if n.isFourNode() {
		left := &LazyNode{
			store:  n.store,
			dirty:  true,
			loaded: true,
			values: []*KV{n.values[0]},
		}
		right := &LazyNode{
			store:  n.store,
			dirty:  true,
			loaded: true,
			values: []*KV{n.values[2]},
		}

		if !n.isLeaf() {

			left.children = []*LazyNode{n.children[0], n.children[1]}
			right.children = []*LazyNode{n.children[2], n.children[3]}
		}

		middle := &LazyNode{
			store:    n.store,
			dirty:    true,
			loaded:   true,
			values:   []*KV{n.values[1]},
			children: []*LazyNode{left, right},
		}

		left.parent = middle
		right.parent = middle

		middle.Insert(key, valueAddr)

		return middle

	}

	if n.isLeaf() {

		if len(n.values) == 0 {
			n.values = []*KV{&KV{key, valueAddr}}
			n.dirty = true
			return nil
		}

		if key < n.values[0].Key {
			n.values = append([]*KV{&KV{key, valueAddr}}, n.values...)
			n.dirty = true
			return nil
		}

		if len(n.values) == 1 {
			n.values = append(n.values, &KV{key, valueAddr})
			n.dirty = true
			return nil
		}

		if key < n.values[1].Key {
			n.values = append(n.values[:1], append([]*KV{&KV{key, valueAddr}}, n.values[1:]...)...)
			n.dirty = true
			return nil
		}

		n.values = append(n.values, &KV{key, valueAddr})
		n.dirty = true
		return nil
	}

	child, _, _ := n.selectChild(key)

	movedUp := child.Insert(key, valueAddr)

	n.dirty = true

	if movedUp != nil {

		v := movedUp.values[0]
		if v.Key < n.values[0].Key {
			n.values = append(movedUp.values, n.values...)
			n.children = append(movedUp.children, n.children[1:]...)
			return nil
		}

		if len(n.values) == 1 {
			n.values = append(n.values, v)
			n.children = append(n.children[:1], movedUp.children...)
			return nil
		}

		if v.Key < n.values[1].Key {
			n.values = append(n.values[:1], append(movedUp.values, n.values[1:]...)...)
			n.children = append(n.children[:1], append(movedUp.children, n.children[2:]...)...)
			return nil
		}

		n.values = append(n.values, v)
		n.children = append(n.children[:2], movedUp.children...)
		return nil

	}

	return nil

}

func (n *LazyNode) valueAndChildIndex(key string) (int, int, int) {
	for i, kv := range n.values {
		if key < kv.Key {
			return i - 1, i, i
		}
	}
	nv := len(n.values)
	return nv - 1, -1, nv
}

func (n *LazyNode) keys() []string {
	keys := []string{}
	for _, k := range n.values {
		keys = append(keys, k.Key)
	}
	return keys
}

func (n *LazyNode) is234Node() error {

	for i := range n.values {
		if i > 0 {
			if n.values[i-1].Key >= n.values[i].Key {
				return fmt.Errorf("keys are not sored: %q >= %q", n.values[i-1].Key, n.values[i].Key)
			}
		}
	}

	if len(n.values) > 3 {
		return fmt.Errorf("More than 3 values! (%d)", len(n.values))
	}
	if len(n.children) == 0 {
		return nil
	}
	if len(n.children) != len(n.values)+1 {

		return fmt.Errorf("Invalid number of children (%d) for number of keys(%d): %#v", len(n.children), len(n.values), n.keys())
	}
	return nil
}

func (n *LazyNode) selectChild(key string) (*LazyNode, *LazyNode, *LazyNode) {

	_, _, ci := n.valueAndChildIndex(key)

	if n.isLeaf() {
		panic("wtf!")
	}

	c := n.children[ci]
	var l *LazyNode

	if ci > 0 {
		l = n.children[ci-1]
	}

	var r *LazyNode
	if ci < len(n.children)-1 {
		r = n.children[ci+1]
	}

	if r != nil {
		r.load()
	}

	if l != nil {
		l.load()
	}

	return c, l, r

}

func (n *LazyNode) Lookup(key string) (uint64, error) {
	n.load()

	for _, kv := range n.values {
		if kv.Key == key {
			return kv.Value, nil
		}
	}

	if !n.isLeaf() {
		c, _, _ := n.selectChild(key)
		return c.Lookup(key)
	}

	return 0, ErrKeyNotFound

}

func (n *LazyNode) ForEach(f func(key string, value uint64) error) error {

	n.load()

	for i, kv := range n.values {
		if !n.isLeaf() {
			err := n.children[i].ForEach(f)
			if err != nil {
				return err
			}
		}
		err := f(kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}

	if !n.isLeaf() {
		err := n.children[len(n.children)-1].ForEach(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *LazyNode) Store() (uint64, error) {
	if !n.dirty {
		return n.addr, nil
	}

	var keys []string
	var refs []uint64

	for _, kv := range n.values {
		keys = append(keys, kv.Key)
		refs = append(refs, kv.Value)
	}

	for _, c := range n.children {
		ca, err := c.Store()
		if err != nil {
			return 0, err
		}
		refs = append(refs, ca)
	}

	data, err := msgpack.Marshal(keys)
	if err != nil {
		return 0, err
	}

	addr, err := n.store.Append(chunk.Pack(chunk.TTFMapNode, refs, data))
	if err != nil {
		return 0, err
	}

	n.dirty = false
	return addr, nil

}

func (n *LazyNode) Validate() error {
	return n.validate(true)
}

func (n *LazyNode) validate(isRoot bool) error {
	n.load()

	if !isRoot && len(n.values) == 0 {
		return errors.New("Empty non-root leaf")
	}

	err := n.is234Node()
	if err != nil {
		return err
	}

	for _, c := range n.children {
		c.load()
	}

	if n.isLeaf() {
		return nil
	}

	topKey := n.values[0].Key
	n.children[0].ForEach(func(key string, value uint64) error {
		if key >= topKey {
			return fmt.Errorf("Key %q should be before %q", key, topKey)
		}
		return nil
	})
	for i := 0; i < len(n.values)-1; i++ {
		lowKey := n.values[i].Key
		topKey := n.values[i+1].Key
		n.children[i+1].ForEach(func(key string, value uint64) error {
			if key >= topKey || key <= lowKey {
				return fmt.Errorf("Key %q should be between %q and %q", key, lowKey, topKey)
			}
			return nil
		})
	}

	lowKey := n.values[len(n.values)-1]
	n.children[len(n.children)-1].ForEach(func(key string, value uint64) error {
		if key >= topKey {
			return fmt.Errorf("Key %q should be after %q", key, lowKey)
		}
		return nil
	})

	for _, c := range n.children {
		err = c.validate(false)
		if err != nil {
			return err
		}
	}

	allKeys := []string{}
	n.ForEach(func(key string, value uint64) error {
		allKeys = append(allKeys, key)
		return nil
	})

	if !sort.IsSorted(sort.StringSlice(allKeys)) {
		return fmt.Errorf("Keys #%v are not sorted", allKeys)
	}

	if len(n.children) == 0 {
		return nil
	}

	depths := []int{}

	n.measureDepth(0, func(d int) {
		depths = append(depths, d)
	})

	firstDepth := depths[0]

	for _, d := range depths {
		if d != firstDepth {
			return fmt.Errorf("Tree is unbalanced!")
		}
	}

	return nil

}

func (n *LazyNode) measureDepth(myDepth int, f func(int)) {
	n.load()
	if n.isLeaf() {
		f(myDepth + 1)
	}
	for _, c := range n.children {
		c.measureDepth(myDepth+1, f)
	}
}

func (n *LazyNode) maxKey() KV {
	n.load()
	if n.isLeaf() {
		return *n.values[len(n.values)-1]
	}
	lastChild := n.children[len(n.children)-1]
	return lastChild.maxKey()
}

func (n *LazyNode) minKey() KV {
	n.load()
	if n.isLeaf() {
		return *n.values[0]
	}
	lastChild := n.children[0]
	return lastChild.minKey()
}

func (n *LazyNode) Delete(key string) error {
	n.load()

	if n.isLeaf() {

		for i, kv := range n.values {
			if kv.Key == key {
				n.values = append(n.values[:i], n.values[i+1:]...)
				n.dirty = true
				return nil
			}
		}

		return nil
	}

	n.dirty = true

	for i, kv := range n.values {
		if kv.Key == key {

			rightChild := n.children[i+1]
			leftChild := n.children[i]

			// 2.1
			if leftChild.NumberOfKeys() > 1 {
				pred := leftChild.maxKey()
				n.values[i] = &pred
				return leftChild.Delete(pred.Key)
			}

			// 2.2: right
			if rightChild.NumberOfKeys() > 1 {
				succ := rightChild.minKey()
				n.values[i] = &succ
				n.dirty = true
				rightChild.dirty = true
				return rightChild.Delete(succ.Key)
			}

			// 2.3
			if leftChild.NumberOfKeys() == 1 && rightChild.NumberOfKeys() == 1 {

				leftChild.values = append(leftChild.values, n.values[i], rightChild.values[0])
				leftChild.dirty = true

				leftChild.children = append(leftChild.children, rightChild.children...)

				n.values = append(n.values[:i], n.values[i+1:]...)
				n.children = append(n.children[:i+1], n.children[i+2:]...)

				// if this was root, replace root with the left child
				if len(n.values) == 0 {
					n.values = leftChild.values
					n.children = leftChild.children
					return n.Delete(key)
				}
				return leftChild.Delete(key)
			}

			return nil
		}
	}

	child, leftSibling, rightSibling := n.selectChild(key)

	if child.NumberOfKeys() == 1 {

		// 3.1: left
		if leftSibling != nil && leftSibling.NumberOfKeys() > 1 {

			lValueIdex, _, _ := n.valueAndChildIndex(key)

			valueFromLeft := leftSibling.values[len(leftSibling.values)-1]

			leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]

			child.values = append([]*KV{n.values[lValueIdex]}, child.values...)

			n.values[lValueIdex] = valueFromLeft

			if !child.isLeaf() {
				child.children = append([]*LazyNode{leftSibling.children[len(leftSibling.children)-1]}, child.children...)
				leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
			}

			leftSibling.dirty = true
			child.dirty = true

			return child.Delete(key)
		}

		// 3.1: right
		if rightSibling != nil && rightSibling.NumberOfKeys() > 1 {

			_, rValueIdex, _ := n.valueAndChildIndex(key)

			valueFromRight := rightSibling.values[0]
			rightSibling.values = rightSibling.values[1:]

			child.values = append(child.values, n.values[rValueIdex])
			n.values[rValueIdex] = valueFromRight
			rightSibling.dirty = true
			child.dirty = true

			if !child.isLeaf() {
				child.children = append(child.children, rightSibling.children[0])
				rightSibling.children = rightSibling.children[1:]
			}

			return child.Delete(key)
		}

		// 3.2 for root
		if n.NumberOfKeys() == 1 {

			if leftSibling != nil && leftSibling.NumberOfKeys() == 1 {
				n.values = append(leftSibling.values, n.values...)
				n.values = append(n.values, child.values...)
				n.children = append(leftSibling.children, child.children...)
				return n.Delete(key)
			}

			if rightSibling != nil && rightSibling.NumberOfKeys() == 1 {
				n.values = append(child.values, n.values...)
				n.values = append(n.values, rightSibling.values...)
				n.children = append(child.children, rightSibling.children...)
				return n.Delete(key)
			}
		}

		// 3.2: left
		if leftSibling != nil && leftSibling.NumberOfKeys() == 1 {
			lValueIndex, _, childIndex := n.valueAndChildIndex(key)
			child.values = append(leftSibling.values, n.values[lValueIndex], child.values[0])

			if !child.isLeaf() {
				child.children = append(leftSibling.children, child.children...)
			}

			n.children = append(n.children[:childIndex-1], n.children[childIndex:]...)
			n.values = append(n.values[:lValueIndex], n.values[lValueIndex+1:]...)

			child.dirty = true
			return child.Delete(key)
		}

		// 3.2: right
		if rightSibling != nil && rightSibling.NumberOfKeys() == 1 {
			_, rValueIndex, childIndex := n.valueAndChildIndex(key)
			child.values = append(child.values, n.values[rValueIndex], rightSibling.values[0])

			if !child.isLeaf() {
				child.children = append(child.children, rightSibling.children...)
			}

			n.children = append(n.children[:childIndex+1], n.children[childIndex+2:]...)
			n.values = append(n.values[:rValueIndex], n.values[rValueIndex+1:]...)
			child.dirty = true
			return child.Delete(key)
		}

	}

	return child.Delete(key)

}

func (n *LazyNode) NumberOfKeys() int {
	n.load()
	return len(n.values)
}
