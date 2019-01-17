package field

import (
	"fmt"
)

// ValueType is the type of a field value.
type ValueType int

// List of supported field value types.
const (
	Unknown ValueType = iota
	NullType
	BoolType
	IntType
	DoubleType
	StringType
	TimeType
)

var (
	// NumValidFieldTypes returns the number of valid field types.
	NumValidFieldTypes = len(validTypes)
)

// IsValid returns true if this is a valid value type.
func (t ValueType) IsValid() bool {
	_, exists := validTypes[t]
	return exists
}

func (t ValueType) String() string {
	switch t {
	case NullType:
		return "null"
	case BoolType:
		return "bool"
	case IntType:
		return "int"
	case DoubleType:
		return "double"
	case StringType:
		return "string"
	case TimeType:
		return "time"
	default:
		return "unknown"
	}
}

// ValueTypeSet is a set of value types.
type ValueTypeSet map[ValueType]struct{}

// Clone clones a value type set.
func (m ValueTypeSet) Clone() ValueTypeSet {
	if len(m) == 0 {
		return nil
	}
	cloned := make(ValueTypeSet, len(m))
	for k := range m {
		cloned[k] = struct{}{}
	}
	return cloned
}

// MergeInPlace merges another value type set into the current set in place.
func (m ValueTypeSet) MergeInPlace(other ValueTypeSet) {
	if len(other) == 0 {
		return
	}
	for k := range other {
		m[k] = struct{}{}
	}
}

var (
	// validTypes is a list of valid value types.
	validTypes = ValueTypeSet{
		NullType:   struct{}{},
		BoolType:   struct{}{},
		IntType:    struct{}{},
		DoubleType: struct{}{},
		StringType: struct{}{},
		TimeType:   struct{}{},
	}

	// OrderableTypes is a list of value types eligible for ordering.
	OrderableTypes = ValueTypeSet{
		NullType:   struct{}{},
		BoolType:   struct{}{},
		IntType:    struct{}{},
		DoubleType: struct{}{},
		StringType: struct{}{},
		TimeType:   struct{}{},
	}
)

// ValueUnion is a value union.
type ValueUnion struct {
	Type         ValueType
	BoolVal      bool
	IntVal       int
	DoubleVal    float64
	StringVal    string
	TimeNanosVal int64
}

var (
	// NullUnion is a null value union.
	NullUnion = ValueUnion{Type: NullType}
)

// BoolAsUnionFn converts a bool to a value union.
type BoolAsUnionFn func(v bool) ValueUnion

// IntAsUnionFn converts an int to a value union.
type IntAsUnionFn func(v int) ValueUnion

// DoubleAsUnionFn converts a double to a value union.
type DoubleAsUnionFn func(v float64) ValueUnion

// StringAsUnionFn converts a string to a value union.
type StringAsUnionFn func(v string) ValueUnion

// TimeAsUnionFn converts a time to a value union.
type TimeAsUnionFn func(v int64) ValueUnion

// NewBoolUnion creates a new bool union.
func NewBoolUnion(v bool) ValueUnion {
	return ValueUnion{
		Type:    BoolType,
		BoolVal: v,
	}
}

// NewIntUnion creates a new int union.
func NewIntUnion(v int) ValueUnion {
	return ValueUnion{
		Type:   IntType,
		IntVal: v,
	}
}

// NewDoubleUnion creates a new double union.
func NewDoubleUnion(v float64) ValueUnion {
	return ValueUnion{
		Type:      DoubleType,
		DoubleVal: v,
	}
}

// NewStringUnion creates a new string union.
func NewStringUnion(v string) ValueUnion {
	return ValueUnion{
		Type:      StringType,
		StringVal: v,
	}
}

// NewTimeUnion creates a new time union.
func NewTimeUnion(v int64) ValueUnion {
	return ValueUnion{
		Type:         TimeType,
		TimeNanosVal: v,
	}
}

// ValueCompareFn compares two value unions.
type ValueCompareFn func(v1, v2 ValueUnion) int

// CompareUnion is a convience method that compares two unions.
// If the two unions have different field types, the result is undefined and the method always returns -1.
// Otherwise, the corresponding values are compared, and the method returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func CompareUnion(v1, v2 ValueUnion) (int, error) {
	if v1.Type != v2.Type {
		return 0, fmt.Errorf("cannot compare unions of different types %v and %v", v1.Type, v2.Type)
	}
	switch v1.Type {
	case NullType:
		return 0, nil
	case BoolType:
		return compareBool(v1.BoolVal, v2.BoolVal), nil
	case IntType:
		return compareInt(v1.IntVal, v2.IntVal), nil
	case DoubleType:
		return compareDouble(v1.DoubleVal, v2.DoubleVal), nil
	case StringType:
		return compareString(v1.StringVal, v2.StringVal), nil
	case TimeType:
		return compareTimeNanos(v1.TimeNanosVal, v2.TimeNanosVal), nil
	default:
		return 0, fmt.Errorf("invalid value type %v", v1.Type)
	}
}

// MustCompareUnion compares two value unions, and panics if it encounters an error.
func MustCompareUnion(v1, v2 ValueUnion) int {
	res, err := CompareUnion(v1, v2)
	if err != nil {
		panic(err)
	}
	return res
}

// MustReverseCompareUnion reverse compares two value unions, and panics if it encounters an error.
func MustReverseCompareUnion(v1, v2 ValueUnion) int {
	return MustCompareUnion(v2, v1)
}

func compareBool(v1, v2 bool) int {
	if v1 == v2 {
		return 0
	}
	if !v1 {
		return -1
	}
	return 1
}

func compareInt(v1, v2 int) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

func compareDouble(v1, v2 float64) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

func compareString(v1, v2 string) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

func compareTimeNanos(v1, v2 int64) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

// ValuesLessThanFn compares two value unions and returns true if `v1` is less than `v2`.
type ValuesLessThanFn func(v1, v2 []ValueUnion) bool

// NewValuesLessThanFn creates a less than fn from a set of field value comparison functions.
// Precondition: len(v1) == len(compareFns) && len(v2) == len(compareFns).
func NewValuesLessThanFn(compareFns []ValueCompareFn) ValuesLessThanFn {
	return func(v1, v2 []ValueUnion) bool {
		for idx, fn := range compareFns {
			res := fn(v1[idx], v2[idx])
			if res < 0 {
				return true
			}
			if res > 0 {
				return false
			}
		}
		return true
	}
}

// FilterValues return a value array excluding the given value indices.
// Precondition: Elements in `toExcludeIndices` are unique, monotonically increasing,
// and within range [0, len(values)).
// Postcondition: `values` is unmodified.
func FilterValues(values []ValueUnion, toExcludeIndices []int) []ValueUnion {
	if len(values) == 0 || len(toExcludeIndices) == 0 {
		return values
	}
	if len(values) == len(toExcludeIndices) {
		return nil
	}
	var (
		valueIdx     = 0
		toExcludeIdx = 0
		res          = make([]ValueUnion, 0, len(values)-len(toExcludeIndices))
	)

	for valueIdx < len(values) && toExcludeIdx < len(toExcludeIndices) {
		if valueIdx == toExcludeIndices[toExcludeIdx] {
			toExcludeIdx++
			valueIdx++
			continue
		}
		res = append(res, values[valueIdx])
		valueIdx++
	}
	if valueIdx < len(values) {
		res = append(res, values[valueIdx:]...)
	}
	return res
}

// Field is an event field.
type Field struct {
	Path  []string
	Value ValueUnion
}

// Iterator iterate over a set of fields.
type Iterator interface {
	// Next returns true if there are more fields to be iterated over,
	// and false otherwise.
	Next() bool

	// Current returns the current field. The field remains valid
	// until the next Next() call.
	Current() Field

	// Close closes the iterator.
	Close()
}
