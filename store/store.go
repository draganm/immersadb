package store

import (
	"github.com/pkg/errors"
	capnp "zombiezen.com/go/capnproto2"
)

type Store []*SegmentFile

func (s Store) Get(a Address) (Segment, error) {
	idx := a.Segment()
	data := s[idx].MMap
	msg, err := capnp.Unmarshal(data[a.Position():])
	if err != nil {
		return Segment{}, err
	}
	return ReadRootSegment(msg)
}

func (s Store) Append(segmentIndex int, f func(s Segment) error) (Address, error) {
	msg, cseg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return NilAddress, errors.Wrap(err, "while creating new capnp message")
	}

	seg, err := NewRootSegment(cseg)

	if err != nil {
		return NilAddress, errors.Wrap(err, "while creating root segment")
	}

	// TODO keep tabs of sizes per layer

	err = f(seg)

	if err != nil {
		return NilAddress, errors.Wrap(err, "while executing segment creator function")
	}

	d, err := msg.Marshal()

	if err != nil {
		return NilAddress, errors.Wrap(err, "while marshallng segment")
	}

	pos, err := s[segmentIndex].Write(d)
	if err != nil {
		return NilAddress, errors.Wrap(err, "whie writing to segment")
	}

	return NewAddress(segmentIndex, pos), nil

}
