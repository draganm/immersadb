package wbbtree

import (
	"encoding/binary"

	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type nodeModifier store.SegmentWriter

func newNodeModifier(s store.Store, key []byte) (nodeModifier, error) {
	sw, err := s.CreateSegment(store.TypeWBBTreeNode, 3, 16+len(key))
	if err != nil {
		return nodeModifier{}, errors.Wrap(err, "while creating segment")
	}
	copy(sw.Data[16:], key)
	return nodeModifier(sw), nil

}

func (n nodeModifier) setLeftCount(c uint64) {
	binary.BigEndian.PutUint64(n.Data, c)
}

func (n nodeModifier) setRightCount(c uint64) {
	binary.BigEndian.PutUint64(n.Data[8:], c)
}

func (n nodeModifier) segmentWriter() store.SegmentWriter {
	return store.SegmentWriter(n)
}

func (n nodeModifier) setLeftChild(lck store.Address) {
	n.segmentWriter().SetChild(0, lck)
}

func (n nodeModifier) setRightChild(rck store.Address) {
	n.segmentWriter().SetChild(1, rck)
}

func (n nodeModifier) setValue(vk store.Address) {
	n.segmentWriter().SetChild(2, vk)
}
