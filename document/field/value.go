package field

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"

	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/compare"

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
	BytesType
	TimeType
)

var (
	// NumValidFieldTypes returns the number of valid field types.
	NumValidFieldTypes = len(validTypes)

	nullBytes = []byte("null")
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
	case BytesType:
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

// MergeTypes merges two value type arrays into a merged value type array.
func MergeTypes(a, b ValueTypeArray) ValueTypeArray {
	m := make(ValueTypeSet, len(a)+len(b))
	for _, t := range a {
		m[t] = struct{}{}
	}
	for _, t := range b {
		m[t] = struct{}{}
	}

	arr := make(ValueTypeArray, 0, len(m))
	for t := range m {
		arr = append(arr, t)
	}
	return arr
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
		BytesType:  struct{}{},
		TimeType:   struct{}{},
	}

	// OrderableTypes is a list of value types eligible for ordering.
	OrderableTypes = ValueTypeSet{
		NullType:   struct{}{},
		BoolType:   struct{}{},
		IntType:    struct{}{},
		DoubleType: struct{}{},
		BytesType:  struct{}{},
		TimeType:   struct{}{},
	}

	// GroupableTypes is a list of value types eligible for grouping.
	GroupableTypes = ValueTypeSet{
		NullType:   struct{}{},
		BoolType:   struct{}{},
		IntType:    struct{}{},
		DoubleType: struct{}{},
		BytesType:  struct{}{},
		TimeType:   struct{}{},
	}
)

// ValueUnion is a value union.
type ValueUnion struct {
	Type         ValueType
	BoolVal      bool
	IntVal       int
	DoubleVal    float64
	BytesVal     []byte
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

// BytesAsUnionFn converts a string to a value union.
type BytesAsUnionFn func(v iterator.Bytes) ValueUnion

// TimeAsUnionFn converts a time to a value union.
type TimeAsUnionFn func(v int64) ValueUnion

// NewValueFromProto creates a value from protobuf message.
func NewValueFromProto(pbValue servicepb.FieldValue) (ValueUnion, error) {
	var v ValueUnion
	switch pbValue.Type {
	case servicepb.FieldValue_NULL:
		v.Type = NullType
	case servicepb.FieldValue_BOOL:
		v.Type = BoolType
		v.BoolVal = pbValue.BoolVal
	case servicepb.FieldValue_INT:
		v.Type = IntType
		v.IntVal = int(pbValue.IntVal)
	case servicepb.FieldValue_DOUBLE:
		v.Type = DoubleType
		v.DoubleVal = pbValue.DoubleVal
	case servicepb.FieldValue_BYTES:
		v.Type = BytesType
		v.BytesVal = pbValue.BytesVal
	case servicepb.FieldValue_TIME:
		v.Type = TimeType
		v.TimeNanosVal = pbValue.TimeNanosVal
	default:
		return v, fmt.Errorf("invalid protobuf field value type %v", pbValue.Type)
	}
	return v, nil
}

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

// NewBytesUnion creates a new string union.
func NewBytesUnion(v iterator.Bytes) ValueUnion {
	return ValueUnion{
		Type:     BytesType,
		BytesVal: v.Data,
	}
}

// NewTimeUnion creates a new time union.
func NewTimeUnion(v int64) ValueUnion {
	return ValueUnion{
		Type:         TimeType,
		TimeNanosVal: v,
	}
}

// MarshalJSON marshals value as a JSON object.
func (v ValueUnion) MarshalJSON() ([]byte, error) {
	switch v.Type {
	case NullType:
		return nullBytes, nil
	case BoolType:
		return json.Marshal(v.BoolVal)
	case IntType:
		return json.Marshal(v.IntVal)
	case DoubleType:
		return json.Marshal(v.DoubleVal)
	case BytesType:
		return json.Marshal(string(v.BytesVal))
	case TimeType:
		return json.Marshal(v.TimeNanosVal)
	default:
		return nil, fmt.Errorf("unknown value type: %v", v.Type)
	}
}

// ToProto converts a value to a value proto message.
func (v *ValueUnion) ToProto() (servicepb.FieldValue, error) {
	var fb servicepb.FieldValue
	switch v.Type {
	case NullType:
		fb.Type = servicepb.FieldValue_NULL
	case BoolType:
		fb.Type = servicepb.FieldValue_BOOL
		fb.BoolVal = v.BoolVal
	case IntType:
		fb.Type = servicepb.FieldValue_INT
		fb.IntVal = int64(v.IntVal)
	case DoubleType:
		fb.Type = servicepb.FieldValue_DOUBLE
		fb.DoubleVal = v.DoubleVal
	case BytesType:
		fb.Type = servicepb.FieldValue_BYTES
		fb.BytesVal = v.BytesVal
	case TimeType:
		fb.Type = servicepb.FieldValue_TIME
		fb.TimeNanosVal = v.TimeNanosVal
	default:
		return servicepb.FieldValue{}, fmt.Errorf("unknown field value type: %v", v.Type)
	}
	return fb, nil
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
	case BytesType:
		return bytes.Equal(v.BytesVal, other.BytesVal)
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
	case BytesType:
		return 31*hash + xxhash.Sum64(v.BytesVal)
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
	case BytesType:
		return bytes.Compare(v1.BytesVal, v2.BytesVal), nil
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

// NewValuesFromProto creates a list of field values from protobuf message.
func NewValuesFromProto(pbValues []servicepb.FieldValue) (Values, error) {
	if len(pbValues) == 0 {
		return nil, nil
	}
	values := make(Values, 0, len(pbValues))
	for _, pbValue := range pbValues {
		value, err := NewValueFromProto(pbValue)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

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

// ToProto converts a value array to a value array protobuf message.
func (v Values) ToProto() ([]servicepb.FieldValue, error) {
	res := make([]servicepb.FieldValue, 0, len(v))
	for _, fv := range v {
		pbFieldValue, err := fv.ToProto()
		if err != nil {
			return nil, err
		}
		res = append(res, pbFieldValue)
	}
	return res, nil
}

// OptionalType is a type "option" which is either "null" or has a valid type.
// It is similar to a `*ValueType` from a functionality perspective but has
// lower GC overhead.
type OptionalType struct {
	HasType bool
	Type    ValueType
}

// MergeInPlace merges the other optional type into the current optional type.
// The merging is valid if:
// - Neither optional types has a type.
// - Only one of `t` and `other` has a valid type.
// - Both optional types have the same valid type.
func (t *OptionalType) MergeInPlace(other OptionalType) error {
	if !other.HasType {
		return nil
	}
	if !t.HasType {
		*t = other
		return nil
	}
	if t.Type != other.Type {
		return fmt.Errorf("merging two incompatible optional types %v and %v", *t, other)
	}
	return nil
}

// OptionalTypeArray is an array of optional types.
type OptionalTypeArray []OptionalType

// MergeInPlace merges the other type array into the current type array in place.
// The other type array becomes invalid after the merge.
// Precondition: One of the following conditions is true:
// - One of or both `v` and `other` are nil.
// - Both type arrays have the same size.
func (v *OptionalTypeArray) MergeInPlace(other OptionalTypeArray) error {
	if len(other) == 0 {
		return nil
	}
	if len(*v) == 0 {
		*v = other
		return nil
	}
	if len(*v) != len(other) {
		return fmt.Errorf("merging two optional type arrays with different sizes %d and %d", len(*v), len(other))
	}
	for i := 0; i < len(other); i++ {
		if err := (*v)[i].MergeInPlace(other[i]); err != nil {
			return err
		}
	}
	return nil
}
