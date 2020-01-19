package wbbtree

import (
	"github.com/draganm/immersadb/store"
	"github.com/pkg/errors"
	capnp "zombiezen.com/go/capnproto2"
)

type nodeModifier struct {
	f store.Segment
	e error
}

func (m *nodeModifier) setError(err error) *nodeModifier {
	m.e = err
	return m
}

func newNodeModifier(f store.Segment) *nodeModifier {
	nm := &nodeModifier{
		f: f,
		e: nil,
	}

	wbtn, err := store.NewWBBTreeNode(f.Segment())
	if err != nil {
		return nm.setError(errors.Wrap(err, "while creating new WBBTreeNode"))
	}

	wbtn.SetCountLeft(0)
	wbtn.SetCountRight(0)

	err = f.Specific().SetWbbtreeNode(wbtn)
	if err != nil {
		return nm.setError(errors.Wrap(err, "while setting WBBTreeNode to Segment"))
	}

	dl, err := capnp.NewUInt64List(f.Segment(), 3)
	if err != nil {
		return nm.setError(errors.Wrap(err, "while creating new data list"))
	}

	for i := 0; i < 3; i++ {
		dl.Set(i, uint64(store.NilAddress))
	}

	err = f.SetChildren(dl)
	if err != nil {
		return nm.setError(errors.Wrap(err, "while setting children of a WBBTreeNode Segment"))
	}

	return nm
}

func (n *nodeModifier) err() error {
	return n.e
}

func (n *nodeModifier) setKey(k []byte) {
	if n.e != nil {
		return
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtreenode"))
		return
	}

	err = tn.SetKey(k)
	if err != nil {
		n.setError(errors.Wrap(err, "while setting key"))
		return
	}
}

func (n *nodeModifier) setLeftCount(c uint64) {
	if n.e != nil {
		return
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtreenode"))
		return
	}

	tn.SetCountLeft(c)
}

func (n *nodeModifier) setRightCount(c uint64) {
	if n.e != nil {
		return
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtreenode"))
		return
	}

	tn.SetCountRight(c)
}

func (n *nodeModifier) setLeftChild(lck store.Address) {
	if n.e != nil {
		return
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting children"))
	}

	ch.Set(0, uint64(lck))
}

func (n *nodeModifier) setRightChild(rck store.Address) {
	if n.e != nil {
		return
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting children"))
	}

	ch.Set(1, uint64(rck))
}

func (n *nodeModifier) setValue(vk store.Address) {
	if n.e != nil {
		return
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting children"))
	}

	ch.Set(2, uint64(vk))
}
