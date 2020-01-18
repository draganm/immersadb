using Go = import "/go.capnp";

@0xd28ad27aef780427;

$Go.package("segment");
$Go.import("segment");

struct Segment {
    children @0 :List(UInt64);
    sizes @1 :List(UInt64);
    specific :union {
        dataLeaf @2 :Data;
        dataNode @3 :UInt64;
        wbbtreeNode @4 :WBBTreeNode;
    }
}

struct WBBTreeNode {
    key @0 :Data;
    countLeft @1 :UInt64;
    countRight @2 :UInt64;
}

