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
	allowed, exists := allowedTypesByOp[f]
	if !exists {
		return nil, fmt.Errorf("calculation op %v does not have allowed types", f)
	}
	return allowed.Clone(), nil
}

// RequiresField returns true if the calculation may be performed against a field
// that differs from the set of grouping fields.
func (f Op) RequiresField() bool {
	return f != Count
}

// NewResult creates a new result from the operator.
func (f Op) NewResult() (Result, error) {
	if !f.IsValid() {
		return nil, fmt.Errorf("invalid calculation operator %v", f)
	}
	newResultFn, exists := newResultFnsByOp[f]
	if !exists {
		return nil, fmt.Errorf("calculation operator %v does not have new result fn", f)
	}
	return newResultFn(), nil
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

	newResultFnsByOp = map[Op]newResultFn{
		Count: newCountResult,
		Sum:   newSumResult,
		Avg:   newAvgResult,
		Min:   newMinResult,
		Max:   newMaxResult,
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
