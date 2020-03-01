package store

import "fmt"

type SegmentType byte

const (
	TypeUndefined SegmentType = iota
	TypeCommit
	TypeDataLeaf
	TypeDataNode
	TypeWBBTreeNode
	TypeTrieNode
)

var segmentTypeNameMap = map[SegmentType]string{
	TypeUndefined:   "Undefined",
	TypeCommit:      "Commit",
	TypeDataLeaf:    "DataLeaf",
	TypeDataNode:    "DataNode",
	TypeWBBTreeNode: "WBBTreeNode",
	TypeTrieNode:    "TrieNode",
}

func (s SegmentType) String() string {
	tp, found := segmentTypeNameMap[s]
	if found {
		return tp
	}

	return fmt.Sprintf("Undefined type %d", s)
}
