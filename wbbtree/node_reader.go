package wbbtree

import (
	"encoding/binary"
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
)

type nodeReader store.SegmentReader

func newNodeReader(st store.Store, a store.Address) (nodeReader, error) {
	sr := st.GetSegment(a)
	if sr.Type() != store.TypeWBBTreeNode {
		return nodeReader{}, errors.New("Segment is not a WBBTreeNode")
	}
	if sr.NumberOfChildren() != 3 {
		return nodeReader{}, errors.New("segment does not have 3 children")
	}

	if len(sr.GetData()) < 16 {
		return nodeReader{}, errors.New("segment must have at least 16 bytes")
	}

	return nodeReader(sr), nil
}

func (n nodeReader) segmentReader() store.SegmentReader {
	return store.SegmentReader(n)
}

func (n nodeReader) leftChild() store.Address {
	return n.segmentReader().GetChildAddress(0)
}

func (n nodeReader) rightChild() store.Address {
	return n.segmentReader().GetChildAddress(1)
}

func (n nodeReader) value() store.Address {
	return n.segmentReader().GetChildAddress(2)
}

func (n nodeReader) key() []byte {
	return n.segmentReader().GetData()[16:]
}

func (n nodeReader) leftCount() uint64 {
	return binary.BigEndian.Uint64(n.segmentReader().GetData())
}

func (n nodeReader) rightCount() uint64 {
	return binary.BigEndian.Uint64(n.segmentReader().GetData()[8:])
}
