using Go = import "/go.capnp";

@0xd28ad27aef780427;

$Go.package("store");
$Go.import("store");

struct Segment {
    children @0 :List(UInt64);
    sizes @1 :List(UInt64);
    specific :union {
        commit @2:Void;
        dataLeaf @3 :Data;
        dataNode @4 :UInt64;
        wbbtreeNode @5 :WBBTreeNode;
    }
}

struct WBBTreeNode {
    key @0 :Data;
    countLeft @1 :UInt64;
    countRight @2 :UInt64;
}

