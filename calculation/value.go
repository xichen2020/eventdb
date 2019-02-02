package calculation

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/compare"
)

// ValueType is the type of a calculation value.
type ValueType int

// A list of supported value types.
const (
	NumberType ValueType = iota
	StringType
)

// ValueUnion is a value union.
type ValueUnion struct {
	Type      ValueType
	NumberVal float64
	StringVal string
}

func newNumberUnion(v float64) ValueUnion {
	return ValueUnion{Type: NumberType, NumberVal: v}
}

func newStringUnion(v string) ValueUnion {
	return ValueUnion{Type: StringType, StringVal: v}
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
		return 0, fmt.Errorf("cannot compare values of different types %v and %v", v1.Type, v2.Type)
	}
	switch v1.Type {
	case NumberType:
		return compare.DoubleCompare(v1.NumberVal, v2.NumberVal), nil
	case StringType:
		return compare.StringCompare(v1.StringVal, v2.StringVal), nil
	}
	panic("should never reach here")
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

// FieldValueToValueFn converts a field value to a calculation value union.
type FieldValueToValueFn func(v *field.ValueUnion) ValueUnion

// AsValueFn returns a function that converts a field value union with the given field
// type to a calculation value union.
func AsValueFn(t field.OptionalType) (FieldValueToValueFn, error) {
	if !t.HasType {
		return nil, nil
	}
	toValueFn, exists := toValueFnsByFieldType[t.Type]
	if !exists {
		return nil, fmt.Errorf("no function exists to convert %v to a calculation value union", t)
	}
	return toValueFn, nil
}

// AsValueFns returns a list of value conversion functions for the given list of field types.
func AsValueFns(fieldTypes field.OptionalTypeArray) ([]FieldValueToValueFn, error) {
	if len(fieldTypes) == 0 {
		return nil, nil
	}
	res := make([]FieldValueToValueFn, 0, len(fieldTypes))
	for _, t := range fieldTypes {
		fn, err := AsValueFn(t)
		if err != nil {
			return nil, err
		}
		res = append(res, fn)
	}
	return res, nil
}

func nullToValue(v *field.ValueUnion) ValueUnion {
	return newNumberUnion(0)
}

func boolToValue(v *field.ValueUnion) ValueUnion {
	var val float64
	if v.BoolVal {
		val = 1
	}
	return newNumberUnion(val)
}

func intToValue(v *field.ValueUnion) ValueUnion {
	return newNumberUnion(float64(v.IntVal))
}

func doubleToValue(v *field.ValueUnion) ValueUnion {
	return newNumberUnion(v.DoubleVal)
}

func stringToValue(v *field.ValueUnion) ValueUnion {
	return newStringUnion(v.StringVal)
}

func timeToValue(v *field.ValueUnion) ValueUnion {
	return newNumberUnion(float64(v.TimeNanosVal))
}

var (
	toValueFnsByFieldType = map[field.ValueType]FieldValueToValueFn{
		field.NullType:   nullToValue,
		field.BoolType:   boolToValue,
		field.IntType:    intToValue,
		field.DoubleType: doubleToValue,
		field.StringType: stringToValue,
		field.TimeType:   timeToValue,
	}
)
