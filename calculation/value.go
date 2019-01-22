package calculation

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
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

// FieldValueToValueFn converts a field value to a calculation value union.
type FieldValueToValueFn func(v *field.ValueUnion) ValueUnion

// AsValueFn returns a function that converts a field value union with the given field
// type to a calculation value union.
func AsValueFn(t field.ValueType) (FieldValueToValueFn, error) {
	toValueFn, exists := toValueFnsByFieldType[t]
	if !exists {
		return nil, fmt.Errorf("no function exists to convert %v to a calculation value union", t)
	}
	return toValueFn, nil
}

// AsValueFns returns a list of value conversion functions for the given list of field types.
func AsValueFns(fieldTypes []field.ValueType) ([]FieldValueToValueFn, error) {
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
