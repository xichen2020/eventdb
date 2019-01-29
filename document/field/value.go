package field

import (
	"fmt"
	"math"

	"github.com/xichen2020/eventdb/x/compare"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/cespare/xxhash"
)

// ValueType is the type of a field value.
type ValueType int

// List of supported field value types.
const (
	UnknownType ValueType = iota
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

// ValueTypeArray is an array of value types.
type ValueTypeArray []ValueType

// Equal returns true if the two value type arrays are considered equal, and false otherwise.
func (arr ValueTypeArray) Equal(other ValueTypeArray) bool {
	if len(arr) != len(other) {
		return false
	}
	for i := 0; i < len(arr); i++ {
		if arr[i] != other[i] {
			return false
		}
	}
	return true
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

	// GroupableTypes is a list of value types eligible for grouping.
	GroupableTypes = ValueTypeSet{
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

// Equal returns true if two value unions are considered equal.
func (v *ValueUnion) Equal(other *ValueUnion) bool {
	if v == nil && other == nil {
		return true
	}
	if v == nil || other == nil {
		return false
	}
	if v.Type != other.Type {
		return false
	}
	switch v.Type {
	case NullType:
		return true
	case BoolType:
		return v.BoolVal == other.BoolVal
	case IntType:
		return v.IntVal == other.IntVal
	case DoubleType:
		return v.DoubleVal == other.DoubleVal
	case StringType:
		return v.StringVal == other.StringVal
	case TimeType:
		return v.TimeNanosVal == other.TimeNanosVal
	}
	return false
}

// Hash returns the hash of a value union.
func (v *ValueUnion) Hash() uint64 {
	// NB(xichen): Follow similar approach in Java for hashing multiple objects together
	// for the purpose of producing fewer collisions.
	hash := uint64(7)
	hash = 31*hash + uint64(v.Type)
	switch v.Type {
	case NullType:
		return 31 * hash
	case BoolType:
		var val int
		if v.BoolVal {
			val = 1
		}
		return 31*hash + uint64(val)
	case IntType:
		return 31*hash + uint64(v.IntVal)
	case DoubleType:
		// NB(xichen): Hashing on bit patterns for doubles might be problematic.
		return 31*hash + math.Float64bits(v.DoubleVal)
	case StringType:
		return 31*hash + xxhash.Sum64(unsafe.ToBytes(v.StringVal))
	case TimeType:
		return 31*hash + uint64(v.TimeNanosVal)
	}
	return hash
}

// ValueCompareFn compares two value unions.
type ValueCompareFn func(v1, v2 ValueUnion) int

// CompareValue is a convience method that compares two value unions.
// If the two values have different field types, the result is undefined and the method always returns -1.
// Otherwise, the corresponding values are compared, and the method returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func CompareValue(v1, v2 ValueUnion) (int, error) {
	if v1.Type != v2.Type {
		return 0, fmt.Errorf("cannot compare unions of different types %v and %v", v1.Type, v2.Type)
	}
	switch v1.Type {
	case NullType:
		return 0, nil
	case BoolType:
		return compare.BoolCompare(v1.BoolVal, v2.BoolVal), nil
	case IntType:
		return compare.IntCompare(v1.IntVal, v2.IntVal), nil
	case DoubleType:
		return compare.DoubleCompare(v1.DoubleVal, v2.DoubleVal), nil
	case StringType:
		return compare.StringCompare(v1.StringVal, v2.StringVal), nil
	case TimeType:
		return compare.TimeCompare(v1.TimeNanosVal, v2.TimeNanosVal), nil
	default:
		return 0, fmt.Errorf("invalid value type %v", v1.Type)
	}
}

// MustCompareValue compares two value unions, and panics if it encounters an error.
func MustCompareValue(v1, v2 ValueUnion) int {
	res, err := CompareValue(v1, v2)
	if err != nil {
		panic(err)
	}
	return res
}

// MustReverseCompareValue reverse compares two value unions, and panics if it encounters an error.
func MustReverseCompareValue(v1, v2 ValueUnion) int {
	return MustCompareValue(v2, v1)
}

// ValuesLessThanFn compares two value unions and returns true if `v1` is less than `v2`.
type ValuesLessThanFn func(v1, v2 Values) bool

// NewValuesLessThanFn creates a less than fn from a set of field value comparison functions.
// The logic is such that the function returned perform a prioritized ordering of results,
// where values at smaller indices of the array have higher priority and values at higher
// indices are only consulted if those at smaller indices are equal.
// Precondition: len(v1) == len(compareFns) && len(v2) == len(compareFns).
func NewValuesLessThanFn(compareFns []ValueCompareFn) ValuesLessThanFn {
	return func(v1, v2 Values) bool {
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
func FilterValues(values Values, toExcludeIndices []int) Values {
	if len(values) == 0 || len(toExcludeIndices) == 0 {
		return values
	}
	if len(values) == len(toExcludeIndices) {
		return nil
	}
	var (
		valueIdx     = 0
		toExcludeIdx = 0
		res          = make(Values, 0, len(values)-len(toExcludeIndices))
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

// Values is an array of values.
type Values []ValueUnion

// Equal returns true if two value arrays are considered equal.
func (v Values) Equal(other Values) bool {
	if len(v) != len(other) {
		return false
	}
	for i := 0; i < len(v); i++ {
		if !v[i].Equal(&other[i]) {
			return false
		}
	}
	return true
}

// Hash returns the hash of a value array.
func (v Values) Hash() uint64 {
	hash := uint64(7)
	for i := 0; i < len(v); i++ {
		hash = 31*hash + v[i].Hash()
	}
	return hash
}

// Clone clones the values.
func (v Values) Clone() Values {
	if len(v) == 0 {
		return nil
	}
	cloned := make(Values, 0, len(v))
	for i := 0; i < len(v); i++ {
		// NB: This is fine as each value union does not contain reference types.
		cloned = append(cloned, v[i])
	}
	return cloned
}
