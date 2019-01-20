package calculation

import (
	"errors"
	"math"
)

// Result represents a merge-able calculation result. In simple cases this can be simply
// a single numeric value. However, there are also cases where calculation of a result
// requires calculations of other intermediate values, which eventually get transformed
// into the final result (e.g., the average value of a field).
type Result interface {
	// Add adds a value to the result.
	Add(v float64)

	// MergeInPlace merges the current result with the other result in place.
	MergeInPlace(other Result) error

	// Value returns the numerical value the result represents.
	Value() float64
}

var (
	nan = math.NaN()

	errMergingDifferentResultTypes = errors.New("merging calculation results with different result types")
)

type newResultFn func() Result

type countResult struct {
	v int
}

func newCountResult() Result {
	return &countResult{}
}

func (r *countResult) Add(float64) { r.v++ }

func (r *countResult) MergeInPlace(other Result) error {
	or, ok := other.(*countResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	r.v += or.v
	return nil
}

func (r *countResult) Value() float64 { return float64(r.v) }

type sumResult struct {
	v float64
}

func newSumResult() Result {
	return &sumResult{}
}

func (r *sumResult) Add(v float64) { r.v += v }

func (r *sumResult) MergeInPlace(other Result) error {
	or, ok := other.(*sumResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	r.v += or.v
	return nil
}

func (r *sumResult) Value() float64 { return r.v }

type avgResult struct {
	s float64
	c int
}

func newAvgResult() Result {
	return &avgResult{}
}

func (r *avgResult) Add(v float64) {
	r.s += v
	r.c++
}

func (r *avgResult) MergeInPlace(other Result) error {
	or, ok := other.(*avgResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	r.s += or.s
	r.c += or.c
	return nil
}

func (r *avgResult) Value() float64 { return r.s / float64(r.c) }

type minResult struct {
	hasValues bool
	v         float64
}

func newMinResult() Result {
	return &minResult{}
}

func (r *minResult) Add(v float64) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v
		return
	}
	if r.v > v {
		r.v = v
	}
}

func (r *minResult) MergeInPlace(other Result) error {
	or, ok := other.(*minResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	if !or.hasValues {
		return nil
	}
	if !r.hasValues {
		*r = *or
		return nil
	}
	if r.v > or.v {
		r.v = or.v
	}
	return nil
}

func (r *minResult) Value() float64 {
	if !r.hasValues {
		return nan
	}
	return r.v
}

type maxResult struct {
	hasValues bool
	v         float64
}

func newMaxResult() Result {
	return &maxResult{}
}

func (r *maxResult) Add(v float64) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v
		return
	}
	if r.v < v {
		r.v = v
	}
}

func (r *maxResult) MergeInPlace(other Result) error {
	or, ok := other.(*maxResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	if !or.hasValues {
		return nil
	}
	if !r.hasValues {
		*r = *or
		return nil
	}
	if r.v < or.v {
		r.v = or.v
	}
	return nil
}

func (r *maxResult) Value() float64 {
	if !r.hasValues {
		return nan
	}
	return r.v
}
