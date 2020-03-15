package store

import "fmt"

type SegmentType byte

const (
	TypeUndefined SegmentType = iota
	TypeCommit
	TypeDataLeaf
	TypeDataNode
	TypebtreeNode
	TypeTrieNode
	TypeBTreeNode
)

var segmentTypeNameMap = map[SegmentType]string{
	TypeUndefined: "Undefined",
	TypeCommit:    "Commit",
	TypeDataLeaf:  "DataLeaf",
	TypeDataNode:  "DataNode",
	TypebtreeNode: "btreeNode",
	TypeTrieNode:  "TrieNode",
	TypeBTreeNode: "BTreeNode",
}

func (s SegmentType) String() string {
	tp, found := segmentTypeNameMap[s]
	if found {
		return tp
	}

	return fmt.Sprintf("Undefined type %d", s)
}
