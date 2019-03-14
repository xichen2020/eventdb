package calculation

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/x/bytes"
	xbytes "github.com/xichen2020/eventdb/x/bytes"
)

// Result represents a merge-able calculation result. In simple cases this can be simply
// a single numeric value or a bytes value. However, there are also cases where calculation
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

	// MarshalJSON marshals the result as a JSON object.
	MarshalJSON() ([]byte, error)
}

var (
	nan = math.NaN()

	errMergingDifferentResultTypes = errors.New("merging calculation results with different result types")
)

// newResultFn creates a new result.
type newResultFn func() Result

// TODO(xichen): Pool the results.

type countResult struct {
	v int
}

// NewCountResult creates a new count result.
func NewCountResult() Result {
	return &countResult{}
}

func (r *countResult) New() Result { return NewCountResult() }

func (r *countResult) Add(ValueUnion) { r.v++ }

func (r *countResult) MergeInPlace(other Result) error {
	or, ok := other.(*countResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	r.v += or.v
	return nil
}

func (r *countResult) Value() ValueUnion { return NewNumberUnion(float64(r.v)) }

func (r *countResult) MarshalJSON() ([]byte, error) { return json.Marshal(r.Value()) }

type sumResult struct {
	v float64
}

// NewSumResult creates a new sum result.
func NewSumResult() Result { return &sumResult{} }

func (r *sumResult) New() Result { return NewSumResult() }

func (r *sumResult) Add(v ValueUnion) { r.v += v.NumberVal }

func (r *sumResult) MergeInPlace(other Result) error {
	or, ok := other.(*sumResult)
	if !ok {
		return errMergingDifferentResultTypes
	}
	r.v += or.v
	return nil
}

func (r *sumResult) Value() ValueUnion { return NewNumberUnion(r.v) }

func (r *sumResult) MarshalJSON() ([]byte, error) { return json.Marshal(r.Value()) }

type avgResult struct {
	s float64
	c int
}

// NewAvgResult creates a new average result.
func NewAvgResult() Result { return &avgResult{} }

func (r *avgResult) New() Result { return NewAvgResult() }

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

func (r *avgResult) Value() ValueUnion { return NewNumberUnion(r.s / float64(r.c)) }

func (r *avgResult) MarshalJSON() ([]byte, error) { return json.Marshal(r.Value()) }

type minNumberResult struct {
	hasValues bool
	v         float64
}

// NewMinNumberResult creates a new minimum number result.
func NewMinNumberResult() Result { return &minNumberResult{} }

func (r *minNumberResult) New() Result { return NewMinNumberResult() }

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
		return NewNumberUnion(nan)
	}
	return NewNumberUnion(r.v)
}

func (r *minNumberResult) MarshalJSON() ([]byte, error) { return json.Marshal(r.Value()) }

type minBytesResult struct {
	hasValues bool
	v         []byte
}

// NewMinBytesResult creates a new minimum bytes result.
func NewMinBytesResult() Result { return &minBytesResult{} }

func (r *minBytesResult) New() Result { return NewMinBytesResult() }

func (r *minBytesResult) Add(v ValueUnion) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v.BytesVal.SafeBytes()
		return
	}
	if xbytes.GreaterThan(r.v, v.BytesVal.Bytes()) {
		r.v = v.BytesVal.SafeBytes()
	}
}

func (r *minBytesResult) MergeInPlace(other Result) error {
	or, ok := other.(*minBytesResult)
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
	if bytes.GreaterThan(r.v, or.v) {
		r.v = or.v
	}
	return nil
}

func (r *minBytesResult) Value() ValueUnion {
	if !r.hasValues {
		return NewBytesUnion(xbytes.NewImmutableBytes(nil))
	}
	return NewBytesUnion(xbytes.NewImmutableBytes(r.v))
}

func (r *minBytesResult) MarshalJSON() ([]byte, error) { return json.Marshal(r.Value()) }

type maxNumberResult struct {
	hasValues bool
	v         float64
}

// NewMaxNumberResult creates a new maximum number result.
func NewMaxNumberResult() Result { return &maxNumberResult{} }

func (r *maxNumberResult) New() Result { return NewMaxNumberResult() }

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
		return NewNumberUnion(nan)
	}
	return NewNumberUnion(r.v)
}

func (r *maxNumberResult) MarshalJSON() ([]byte, error) { return json.Marshal(r.Value()) }

type maxBytesResult struct {
	hasValues bool
	v         []byte
}

// NewMaxBytesResult creates a new maximum bytes result.
func NewMaxBytesResult() Result { return &maxBytesResult{} }

func (r *maxBytesResult) New() Result { return NewMaxBytesResult() }

func (r *maxBytesResult) Add(v ValueUnion) {
	if !r.hasValues {
		r.hasValues = true
		r.v = v.BytesVal.SafeBytes()
		return
	}
	if xbytes.LessThan(r.v, v.BytesVal.Bytes()) {
		r.v = v.BytesVal.SafeBytes()
	}
}

func (r *maxBytesResult) MergeInPlace(other Result) error {
	or, ok := other.(*maxBytesResult)
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
	if bytes.LessThan(r.v, or.v) {
		r.v = or.v
	}
	return nil
}

func (r *maxBytesResult) Value() ValueUnion {
	if !r.hasValues {
		return NewBytesUnion(xbytes.NewImmutableBytes(nil))
	}
	return NewBytesUnion(xbytes.NewImmutableBytes(r.v))
}

func (r *maxBytesResult) MarshalJSON() ([]byte, error) { return json.Marshal(r.Value()) }

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

// ToProto converts a result array to a value array protobuf message.
func (arr ResultArray) ToProto() []servicepb.CalculationValue {
	if len(arr) == 0 {
		return nil
	}
	values := make([]servicepb.CalculationValue, 0, len(arr))
	for _, res := range arr {
		values = append(values, res.Value().ToProto())
	}
	return values
}

// NewResultArrayFromValueTypesFn creates a new result array based on the field value types.
type NewResultArrayFromValueTypesFn func(valueTypes field.OptionalTypeArray) (ResultArray, error)

// NewResultArrayFn creates a new result array.
type NewResultArrayFn func() (ResultArray, error)
