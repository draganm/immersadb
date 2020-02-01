package store

import (
	"github.com/pkg/errors"
)

type LayerGCPlanStep int

const (
	UnknownGCPlan LayerGCPlanStep = iota
	PushDown
	Keep
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

	if int64(newBytes)+s[1].lastSegmentPosition > s[1].limit {
		l1GarbageBytes := s[1].lastSegmentPosition - int64(sr.GetLayerTotalSize(1))
		if l1GarbageBytes > int64(newBytes) {
			plan[1] = Compact
		} else {
			plan[1] = PushDown
		}
	}

	ns, err := s.newStoreFromPlan(plan)
	if err != nil {
		return NilAddress, nil, errors.Wrap(err, "while creating new store")
	}

	newRoot, err := executeGCPlan(s, ns, root, plan)
	if err != nil {
		return NilAddress, nil, errors.Wrap(err, "while executing plan")
	}
	return newRoot, ns, nil
}

func executeGCPlan(s, ns Store, a Address, plan []LayerGCPlanStep) (Address, error) {

	planStep := plan[a.Segment()]

	switch planStep {
	case Keep:
		return a, nil
	case PushDown:
		// push down
		sr := s.GetSegment(a)
		nc := sr.NumberOfChildren()

		children := []Address{}

		for i := 0; i < nc; i++ {
			ca := sr.GetChildAddress(i)
			if ca != NilAddress {
				nca, err := executeGCPlan(s, ns, ca, plan)
				if err != nil {
					return NilAddress, err
				}
				children = append(children, nca)
			}
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
			if ca != NilAddress {
				nca, err := executeGCPlan(s, ns, ca, plan)
				if err != nil {
					return NilAddress, err
				}
				children = append(children, nca)
			}
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
