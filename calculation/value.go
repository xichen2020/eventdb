package calculation

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	xbytes "github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/compare"
)

// ValueType is the type of a calculation value.
type ValueType int

// A list of supported value types.
const (
	NumberType ValueType = iota
	BytesType
)

var (
	nullBytes = []byte("null")
)

// ValueUnion is a value union.
type ValueUnion struct {
	Type      ValueType
	NumberVal float64
	BytesVal  xbytes.Bytes
}

// NewValueFromProto creates a value from protobuf message.
func NewValueFromProto(pbValue servicepb.CalculationValue) (ValueUnion, error) {
	var v ValueUnion
	switch pbValue.Type {
	case servicepb.CalculationValue_NUMBER:
		v.Type = NumberType
		v.NumberVal = pbValue.NumberVal
	case servicepb.CalculationValue_BYTES:
		v.Type = BytesType
		v.BytesVal = xbytes.NewImmutableBytes(pbValue.BytesVal)
	default:
		return v, fmt.Errorf("invalid protobuf calculation value type %v", pbValue.Type)
	}
	return v, nil
}

// MarshalJSON marshals value as a JSON object.
func (u ValueUnion) MarshalJSON() ([]byte, error) {
	switch u.Type {
	case NumberType:
		// NaN cannot be marshalled as JSON, so marshal it as null.
		if math.IsNaN(u.NumberVal) {
			return nullBytes, nil
		}
		return json.Marshal(u.NumberVal)
	case BytesType:
		return json.Marshal(u.BytesVal)
	default:
		return nil, fmt.Errorf("unexpected value type %v", u.Type)
	}
}

// ToProto converts a value to a calculation value protobuf message.
func (u ValueUnion) ToProto() servicepb.CalculationValue {
	switch u.Type {
	case NumberType:
		return servicepb.CalculationValue{
			Type:      servicepb.CalculationValue_NUMBER,
			NumberVal: u.NumberVal,
		}
	case BytesType:
		return servicepb.CalculationValue{
			Type:     servicepb.CalculationValue_BYTES,
			BytesVal: u.BytesVal.Bytes(),
		}
	default:
		panic(fmt.Errorf("unexpected calculation value type %v", u.Type))
	}
}

// NewNumberUnion creates a new number union.
func NewNumberUnion(v float64) ValueUnion {
	return ValueUnion{Type: NumberType, NumberVal: v}
}

// NewBytesUnion creates a new string union.
func NewBytesUnion(v xbytes.Bytes) ValueUnion {
	return ValueUnion{Type: BytesType, BytesVal: v}
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
	case BytesType:
		return compare.BytesCompare(v1.BytesVal, v2.BytesVal), nil
	default:
		panic("should never reach here")
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
	return NewNumberUnion(0)
}

func boolToValue(v *field.ValueUnion) ValueUnion {
	var val float64
	if v.BoolVal {
		val = 1
	}
	return NewNumberUnion(val)
}

func intToValue(v *field.ValueUnion) ValueUnion {
	return NewNumberUnion(float64(v.IntVal))
}

func doubleToValue(v *field.ValueUnion) ValueUnion {
	return NewNumberUnion(v.DoubleVal)
}

func bytesToValue(v *field.ValueUnion) ValueUnion {
	return NewBytesUnion(v.BytesVal)
}

func timeToValue(v *field.ValueUnion) ValueUnion {
	return NewNumberUnion(float64(v.TimeNanosVal))
}

// Values is a list of calculation values.
type Values []ValueUnion

// NewValuesFromProto creates a list of calculation values from protobuf message.
func NewValuesFromProto(pbValues []servicepb.CalculationValue) (Values, error) {
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

var (
	toValueFnsByFieldType = map[field.ValueType]FieldValueToValueFn{
		field.NullType:   nullToValue,
		field.BoolType:   boolToValue,
		field.IntType:    intToValue,
		field.DoubleType: doubleToValue,
		field.BytesType:  bytesToValue,
		field.TimeType:   timeToValue,
	}
)
