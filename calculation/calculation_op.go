package calculation

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
)

// Op represents a calculation operator.
type Op int

// A list of supported calculation operators.
const (
	UnknownOp Op = iota
	Count
	Sum
	Avg
	Min
	Max
)

// newOp creates a new calculation operator from a string.
func newOp(str string) (Op, error) {
	if f, exists := stringToOps[str]; exists {
		return f, nil
	}
	return UnknownOp, fmt.Errorf("unknown calculation op string: %s", str)
}

// IsValid returns true if a calculation operator is valid.
func (f Op) IsValid() bool {
	_, exists := validFilterOps[f]
	return exists
}

// AllowedTypes returns a list of value types that are allowed
// for the given calculation operator.
func (f Op) AllowedTypes() (field.ValueTypeSet, error) {
	if !f.IsValid() {
		return nil, fmt.Errorf("invalid calculation operator %v", f)
	}
	allowedTypes, exists := allowedTypesByOp[f]
	if !exists {
		return nil, fmt.Errorf("calculation op %v does not have allowed types", f)
	}
	return allowedTypes.Clone(), nil
}

// RequiresField returns true if the calculation may be performed against a field
// that differs from the set of grouping fields.
func (f Op) RequiresField() bool {
	return f != Count
}

// NewResult creates a new result based on the operator and possibly the given field value type.
func (f Op) NewResult(t field.ValueType) (Result, error) {
	if !f.IsValid() {
		return nil, fmt.Errorf("invalid calculation operator %v", f)
	}

	if !f.RequiresField() {
		// The operator does not require a field, and as such the value type is not examined.
		newResultFn, exists := newResultFnsByOpsNoType[f]
		if !exists {
			return nil, fmt.Errorf("calculation operator %v does not have new result fn", f)
		}
		return newResultFn(), nil
	}

	// Otherwise examine both the operator and the field value type.
	newResultFnsByType, exists := newResultFnsByOpsAndType[f]
	if !exists {
		return nil, fmt.Errorf("calculation operator %v does not have new result fn", f)
	}
	newResultFn, exists := newResultFnsByType[t]
	if !exists {
		return nil, fmt.Errorf("calculation operator %v does not have new result fn for value type %v", f, t)
	}
	return newResultFn(), nil
}

// MustNewResult creates a new result based on the operator and possibly the given field value type,
// and panics if an error is encountered.
func (f Op) MustNewResult(t field.ValueType) Result {
	r, err := f.NewResult(t)
	if err != nil {
		panic(err)
	}
	return r
}

// String returns the string representation of the calculation operator.
func (f Op) String() string {
	if s, exists := opStrings[f]; exists {
		return s
	}
	// nolint: goconst
	return "unknown"
}

// UnmarshalJSON unmarshals a JSON object as a calculation operator.
func (f *Op) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	op, err := newOp(str)
	if err != nil {
		return err
	}
	*f = op
	return nil
}

var (
	validFilterOps = map[Op]struct{}{
		Count: struct{}{},
		Sum:   struct{}{},
		Avg:   struct{}{},
		Min:   struct{}{},
		Max:   struct{}{},
	}
	opStrings = map[Op]string{
		Count: "COUNT",
		Sum:   "SUM",
		Avg:   "AVG",
		Min:   "MIN",
		Max:   "MAX",
	}
	stringToOps map[string]Op

	// For operators that do not require fields.
	newResultFnsByOpsNoType = map[Op]newResultFn{
		Count: newCountResult,
	}

	// For operators that require fields.
	newResultFnsByOpsAndType = map[Op]map[field.ValueType]newResultFn{
		Sum: map[field.ValueType]newResultFn{
			field.IntType:    newSumResult,
			field.DoubleType: newSumResult,
			field.TimeType:   newSumResult,
		},
		Avg: map[field.ValueType]newResultFn{
			field.IntType:    newAvgResult,
			field.DoubleType: newAvgResult,
			field.TimeType:   newAvgResult,
		},
		Min: map[field.ValueType]newResultFn{
			field.IntType:    newMinNumberResult,
			field.DoubleType: newMinNumberResult,
			field.StringType: newMinStringResult,
			field.TimeType:   newMinNumberResult,
		},
		Max: map[field.ValueType]newResultFn{
			field.IntType:    newMaxNumberResult,
			field.DoubleType: newMaxNumberResult,
			field.StringType: newMaxStringResult,
			field.TimeType:   newMaxNumberResult,
		},
	}

	allowedTypesByOp = map[Op]field.ValueTypeSet{
		Count: field.ValueTypeSet{
			field.NullType:   struct{}{},
			field.BoolType:   struct{}{},
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.StringType: struct{}{},
			field.TimeType:   struct{}{},
		},
		Sum: field.ValueTypeSet{
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.TimeType:   struct{}{},
		},
		Avg: field.ValueTypeSet{
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.TimeType:   struct{}{},
		},
		Min: field.ValueTypeSet{
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.StringType: struct{}{},
			field.TimeType:   struct{}{},
		},
		Max: field.ValueTypeSet{
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.StringType: struct{}{},
			field.TimeType:   struct{}{},
		},
	}
)

func init() {
	stringToOps = make(map[string]Op, len(opStrings))
	for k, v := range opStrings {
		stringToOps[v] = k
	}
}
