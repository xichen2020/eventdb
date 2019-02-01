package calculation

import (
	"errors"
	"fmt"
	"math"

	"github.com/xichen2020/eventdb/document/field"
)

// Result represents a merge-able calculation result. In simple cases this can be simply
// a single numeric value or a string value. However, there are also cases where calculation
// of a result requires calculations of other intermediate values, which eventually get
// transformed into the final result (e.g., the average value of a field).
type Result interface {
	// New creates a new result with the same result type.
	New() Result

	// Add adds a value to the result.
	// TODO(xichen): Perhaps make this API take a `*ValueUnion` instead to save copy time.
	Add(v ValueUnion)

	// MergeInPlace merges the current result with the other result in place.
	MergeInPlace(other Result) error

	// Value returns the result value.
	Value() ValueUnion
}

var (
	nan         = math.NaN()
	emptyString string

	errMergingDifferentResultTypes = errors.New("merging calculation results with different result types")
)

// newResultFn creates a new result.
type newResultFn func() Result

// TODO(xichen): Pool the results.

type countResult struct {
	v int
}

func newCountResult() Result {
	return &countResult{}
}

func (r *countResult) New() Result { return newCountResult() }

func (r *countResult) Add(ValueUnion) { r.v++ }

func (r *countResult) MergeInPlace(other Result) error {
	or, ok := other.(*countResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	r.v += or.v
	return nil
}

func (r *countResult) Value() ValueUnion { return newNumberUnion(float64(r.v)) }

type sumResult struct {
	v float64
}

func newSumResult() Result {
	return &sumResult{}
}

func (r *sumResult) New() Result { return newSumResult() }

func (r *sumResult) Add(v ValueUnion) { r.v += v.NumberVal }

func (r *sumResult) MergeInPlace(other Result) error {
	or, ok := other.(*sumResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	r.v += or.v
	return nil
}

func (r *sumResult) Value() ValueUnion { return newNumberUnion(r.v) }

type avgResult struct {
	s float64
	c int
}

func newAvgResult() Result {
	return &avgResult{}
}

func (r *avgResult) New() Result { return newAvgResult() }

func (r *avgResult) Add(v ValueUnion) {
	r.s += v.NumberVal
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

func (r *avgResult) Value() ValueUnion { return newNumberUnion(r.s / float64(r.c)) }

type minNumberResult struct {
	hasValues bool
	v         float64
}

func newMinNumberResult() Result {
	return &minNumberResult{}
}

func (r *minNumberResult) New() Result { return newMinNumberResult() }

func (r *minNumberResult) Add(v ValueUnion) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v.NumberVal
		return
	}
	if r.v > v.NumberVal {
		r.v = v.NumberVal
	}
}

func (r *minNumberResult) MergeInPlace(other Result) error {
	or, ok := other.(*minNumberResult)
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

func (r *minNumberResult) Value() ValueUnion {
	if !r.hasValues {
		return newNumberUnion(nan)
	}
	return newNumberUnion(r.v)
}

type minStringResult struct {
	hasValues bool
	v         string
}

func newMinStringResult() Result {
	return &minStringResult{}
}

func (r *minStringResult) New() Result { return newMinStringResult() }

func (r *minStringResult) Add(v ValueUnion) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v.StringVal
		return
	}
	if r.v > v.StringVal {
		r.v = v.StringVal
	}
}

func (r *minStringResult) MergeInPlace(other Result) error {
	or, ok := other.(*minStringResult)
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

func (r *minStringResult) Value() ValueUnion {
	if !r.hasValues {
		return newStringUnion(emptyString)
	}
	return newStringUnion(r.v)
}

type maxNumberResult struct {
	hasValues bool
	v         float64
}

func newMaxNumberResult() Result {
	return &maxNumberResult{}
}

func (r *maxNumberResult) New() Result { return newMaxNumberResult() }

func (r *maxNumberResult) Add(v ValueUnion) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v.NumberVal
		return
	}
	if r.v < v.NumberVal {
		r.v = v.NumberVal
	}
}

func (r *maxNumberResult) MergeInPlace(other Result) error {
	or, ok := other.(*maxNumberResult)
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

func (r *maxNumberResult) Value() ValueUnion {
	if !r.hasValues {
		return newNumberUnion(nan)
	}
	return newNumberUnion(r.v)
}

type maxStringResult struct {
	hasValues bool
	v         string
}

func newMaxStringResult() Result {
	return &maxStringResult{}
}

func (r *maxStringResult) New() Result { return newMaxStringResult() }

func (r *maxStringResult) Add(v ValueUnion) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v.StringVal
		return
	}
	if r.v < v.StringVal {
		r.v = v.StringVal
	}
}

func (r *maxStringResult) MergeInPlace(other Result) error {
	or, ok := other.(*maxStringResult)
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

func (r *maxStringResult) Value() ValueUnion {
	if !r.hasValues {
		return newStringUnion(emptyString)
	}
	return newStringUnion(r.v)
}

// ResultArray is an array of calculation result.
type ResultArray []Result

// New creates a new result array where each result is created anew from
// the existing result in the corresponding slot.
func (arr ResultArray) New() ResultArray {
	if len(arr) == 0 {
		return nil
	}
	resArray := make(ResultArray, 0, len(arr))
	for _, res := range arr {
		if res == nil {
			resArray = append(resArray, nil)
			continue
		}
		resArray = append(resArray, res.New())
	}
	return resArray
}

// MergeInPlace merges the other result array into the current array in place.
// Precondition: len(arr) == len(other).
func (arr ResultArray) MergeInPlace(other ResultArray) {
	if len(arr) != len(other) {
		panic(fmt.Errorf("merging two calculation result arrays with different lengths: %d and %d", len(arr), len(other)))
	}
	for i := 0; i < len(arr); i++ {
		arr[i].MergeInPlace(other[i])
	}
}

// NewResultArrayFromValueTypesFn creates a new result array based on the field value types.
type NewResultArrayFromValueTypesFn func(valueTypes field.OptionalTypeArray) (ResultArray, error)

// NewResultArrayFn creates a new result array.
type NewResultArrayFn func() (ResultArray, error)
