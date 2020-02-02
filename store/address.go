package store

import "fmt"

type Address uint64

func (a Address) Position() uint64 {
	return uint64(a) << 2 >> 2
}

func (a Address) Segment() int {
	ui := uint64(a) >> 62
	return int(ui)
}

func NewAddress(segment int, position uint64) Address {
	return Address(uint64(segment&0x3)<<62 | (position << 2 >> 2))
}

const NilAddress Address = (0xffffffffffffffff)

const MaxLayers = 4

func (a Address) String() string {
	if a == NilAddress {
		return "NILAddress"
	}
	return fmt.Sprintf("Segment %d Position %d", a.Segment(), a.Position())
}
