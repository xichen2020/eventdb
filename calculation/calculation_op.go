package calculation

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
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
	if s, exists := opBytess[f]; exists {
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

// ToOptionalProto converts the calculation op to an optional calculation op protobuf message.
func (f *Op) ToOptionalProto() (servicepb.OptionalCalculationOp, error) {
	if f == nil {
		noValue := &servicepb.OptionalCalculationOp_NoValue{NoValue: true}
		return servicepb.OptionalCalculationOp{Value: noValue}, nil
	}
	v, err := f.ToProto()
	if err != nil {
		return servicepb.OptionalCalculationOp{}, err
	}
	return servicepb.OptionalCalculationOp{
		Value: &servicepb.OptionalCalculationOp_Data{Data: v},
	}, nil
}

// ToProto converts the calculation op to a calculation op protobuf message.
func (f Op) ToProto() (servicepb.Calculation_Op, error) {
	switch f {
	case Count:
		return servicepb.Calculation_COUNT, nil
	case Sum:
		return servicepb.Calculation_SUM, nil
	case Avg:
		return servicepb.Calculation_AVG, nil
	case Min:
		return servicepb.Calculation_MIN, nil
	case Max:
		return servicepb.Calculation_MAX, nil
	default:
		return servicepb.Calculation_UNKNOWNOP, fmt.Errorf("invalid calculation op %v", f)
	}
}

var (
	validFilterOps = map[Op]struct{}{
		Count: struct{}{},
		Sum:   struct{}{},
		Avg:   struct{}{},
		Min:   struct{}{},
		Max:   struct{}{},
	}
	opBytess = map[Op]string{
		Count: "COUNT",
		Sum:   "SUM",
		Avg:   "AVG",
		Min:   "MIN",
		Max:   "MAX",
	}
	stringToOps map[string]Op

	// For operators that do not require fields.
	newResultFnsByOpsNoType = map[Op]newResultFn{
		Count: NewCountResult,
	}

	// For operators that require fields.
	newResultFnsByOpsAndType = map[Op]map[field.ValueType]newResultFn{
		Sum: map[field.ValueType]newResultFn{
			field.IntType:    NewSumResult,
			field.DoubleType: NewSumResult,
			field.TimeType:   NewSumResult,
		},
		Avg: map[field.ValueType]newResultFn{
			field.IntType:    NewAvgResult,
			field.DoubleType: NewAvgResult,
			field.TimeType:   NewAvgResult,
		},
		Min: map[field.ValueType]newResultFn{
			field.IntType:    NewMinNumberResult,
			field.DoubleType: NewMinNumberResult,
			field.BytesType:  NewMinBytesResult,
			field.TimeType:   NewMinNumberResult,
		},
		Max: map[field.ValueType]newResultFn{
			field.IntType:    NewMaxNumberResult,
			field.DoubleType: NewMaxNumberResult,
			field.BytesType:  NewMaxBytesResult,
			field.TimeType:   NewMaxNumberResult,
		},
	}

	allowedTypesByOp = map[Op]field.ValueTypeSet{
		Count: field.ValueTypeSet{
			field.NullType:   struct{}{},
			field.BoolType:   struct{}{},
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.BytesType:  struct{}{},
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
			field.BytesType:  struct{}{},
			field.TimeType:   struct{}{},
		},
		Max: field.ValueTypeSet{
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.BytesType:  struct{}{},
			field.TimeType:   struct{}{},
		},
	}
)

func init() {
	stringToOps = make(map[string]Op, len(opBytess))
	for k, v := range opBytess {
		stringToOps[v] = k
	}
}
