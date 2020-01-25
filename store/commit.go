package store

import "github.com/pkg/errors"

type LayerGCPlanStep int

const (
	UnknownGCPlan LayerGCPlanStep = iota
	PushDown
	Keep
	// Compact
)

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

	ns := s
	newRoot, err := executeGCPlan(s, ns, root, plan)
	if err != nil {
		return NilAddress, nil, errors.Wrap(err, "while executing plan")
	}
	return newRoot, ns, nil
}

func executeGCPlan(s, ns Store, a Address, plan []LayerGCPlanStep) (Address, error) {

	planStep := plan[a.Segment()]
	if planStep == Keep {
		return a, nil
	}

	if planStep != PushDown {
		return NilAddress, errors.Errorf("Unsupported plan step %d", planStep)
	}

	sr := s.GetSegment(a)

	nc := sr.NumberOfChildren()

	d := sr.GetData()
	wr, err := ns.CreateSegment(a.Segment()+1, sr.Type(), nc, len(d))
	if err != nil {
		return NilAddress, errors.Wrapf(err, "while creating segment on layer %d", a.Segment()+1)
	}

	copy(wr.Data, d)

	for i := 0; i < nc; i++ {
		ca := sr.GetChildAddress(i)
		if ca != NilAddress {
			nca, err := executeGCPlan(s, ns, ca, plan)
			if err != nil {
				return NilAddress, err
			}
			wr.SetChild(i, nca)
		}
	}

	return wr.Address, nil
}
