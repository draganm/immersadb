package store

import (
	"github.com/pkg/errors"
)

type LayerGCPlanStep int

const (
	UnknownGCPlan LayerGCPlanStep = iota
	Keep
	PushDown
	Compact
)

func (s Store) newStoreFromPlan(steps []LayerGCPlanStep) (Store, error) {
	ns := make(Store, 4)

	for i := 1; i < len(steps); i++ {
		switch steps[i] {
		case UnknownGCPlan, Keep:
			ns[i] = s[i]
			continue
		case PushDown, Compact:
			nsf, err := s[i].CreateEmptySibling()
			if err != nil {
				return nil, errors.Wrap(err, "while creating new empty sibling")
			}
			ns[i] = nsf
		}
	}

	return ns, nil
}

func (s Store) Commit(root Address) (Address, Store, error) {

	if root.Segment() != 0 {
		return NilAddress, nil, errors.New("root is not in layer 0")
	}

	plan := []LayerGCPlanStep{
		PushDown,
		Keep,
		Keep,
		Keep,
	}

	sr := s.GetSegment(root)

	newBytes := sr.GetLayerTotalSize(0)

	if !s[1].CanAppend(newBytes) {
		l1GarbageBytes := uint64(s[1].nextFreeByte) - sr.GetLayerTotalSize(1)
		if l1GarbageBytes+s[1].RemainingCapacity() >= newBytes && (float64(l1GarbageBytes)/float64(s[1].maxSize) >= 0.1) {
			plan[1] = Compact
		} else {
			plan[1] = PushDown
		}
	}

	if plan[1] == PushDown {

		l1Bytes := sr.GetLayerTotalSize(1)
		if !s[2].CanAppend(l1Bytes) {
			l2GarbageBytes := uint64(s[2].nextFreeByte) - (sr.GetLayerTotalSize(2))
			if l2GarbageBytes+s[2].RemainingCapacity() >= l1Bytes && (float64(l2GarbageBytes)/float64(s[2].maxSize) >= 0.1) {
				plan[2] = Compact
			} else {
				plan[2] = PushDown
			}

		}

	}

	if plan[2] == PushDown {

		l2Bytes := sr.GetLayerTotalSize(2)
		if !s[3].CanAppend(l2Bytes) {
			l3GarbageBytes := uint64(s[3].nextFreeByte) - (sr.GetLayerTotalSize(3))
			if l3GarbageBytes+s[3].RemainingCapacity() >= l2Bytes {
				plan[2] = Compact
			} else {
				return NilAddress, nil, errors.New("database is full")
			}

		}

	}

	ns, err := s.newStoreFromPlan(plan)
	if err != nil {
		return NilAddress, nil, errors.Wrap(err, "while creating new store")
	}

	ns.StartUse()

	newRoot, err := executeGCPlan(s, ns, root, plan)
	if err != nil {
		return NilAddress, nil, errors.Wrap(err, "while executing plan")
	}

	return newRoot, ns, nil
}

func executeGCPlan(s, ns Store, a Address, plan []LayerGCPlanStep) (Address, error) {

	if a == NilAddress {
		return NilAddress, nil
	}

	planStep := plan[a.Segment()]

	switch planStep {
	case Keep:
		return a, nil
	case PushDown:
		sr := s.GetSegment(a)
		nc := sr.NumberOfChildren()

		children := []Address{}

		for i := 0; i < nc; i++ {
			ca := sr.GetChildAddress(i)
			nca, err := executeGCPlan(s, ns, ca, plan)
			if err != nil {
				return NilAddress, err
			}
			children = append(children, nca)
		}

		d := sr.GetData()
		wr, err := ns.CreateSegment(a.Segment()+1, sr.Type(), nc, len(d))
		if err != nil {
			return NilAddress, errors.Wrapf(err, "while creating segment on layer %d", a.Segment()+1)
		}

		for i, ch := range children {
			wr.SetChild(i, ch)
		}

		copy(wr.Data, d)

		return wr.Address, nil

	case Compact:

		sr := s.GetSegment(a)
		nc := sr.NumberOfChildren()

		children := []Address{}

		for i := 0; i < nc; i++ {
			ca := sr.GetChildAddress(i)
			nca, err := executeGCPlan(s, ns, ca, plan)
			if err != nil {
				return NilAddress, err
			}
			children = append(children, nca)
		}

		d := sr.GetData()
		wr, err := ns.CreateSegment(a.Segment(), sr.Type(), nc, len(d))
		if err != nil {
			return NilAddress, errors.Wrapf(err, "while creating segment on layer %d", a.Segment()+1)
		}

		for i, ch := range children {
			wr.SetChild(i, ch)
		}

		copy(wr.Data, d)

		return wr.Address, nil

	default:
		return NilAddress, errors.Errorf("Unsupported plan step %d", planStep)
	}

}
