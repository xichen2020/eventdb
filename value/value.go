package value

import (
	"fmt"
	"strconv"
)

var (
	nullValue  = &Value{t: NullType}
	trueValue  = &Value{t: BoolType, b: true}
	falseValue = &Value{t: BoolType, b: false}
)

// Value is a union of different types of values. There is at most one value
// in the union that is active at any point in time. The active value is
// determined by the type field in the value.
// TODO(xichen): add close method to return value to pool.
type Value struct {
	t Type
	b bool
	s string
	n float64
	a []*Value
	o Object
}

// NewObjectValue creates a new object value.
func NewObjectValue(o Object) *Value {
	var v Value
	v.setObject(o)
	return &v
}

// NewArrayValue creates a new array value.
func NewArrayValue(a []*Value) *Value {
	var v Value
	v.setArray(a)
	return &v
}

// NewStringValue creates a new string value.
func NewStringValue(s string) *Value {
	var v Value
	v.setString(s)
	return &v
}

// NewNumberValue creates a new number value.
func NewNumberValue(n float64) *Value {
	var v Value
	v.setNumber(n)
	return &v
}

// NewBoolValue creates a new boolean value.
func NewBoolValue(b bool) *Value {
	if b {
		return trueValue
	}
	return falseValue
}

// NewNullValue creates a new null value.
func NewNullValue() *Value { return nullValue }

// Type returns the value type.
func (v *Value) Type() Type { return v.t }

// Object returns an object value if the value type is object, or an error otherwise.
func (v *Value) Object() (Object, error) {
	if v.t != ObjectType {
		return Object{}, fmt.Errorf("expect object type but got %v", v.t)
	}
	return v.o, nil
}

// MustObject returns an object value or panics if the value type is not object.
func (v *Value) MustObject() Object {
	o, err := v.Object()
	if err != nil {
		panic(err)
	}
	return o
}

// Array returns an array value if the value type is array, or an error otherwise.
func (v *Value) Array() ([]*Value, error) {
	if v.t != ArrayType {
		return nil, fmt.Errorf("expect array type but got %v", v.t)
	}
	return v.a, nil
}

// MustArray returns an array value or panics if the value type is not array.
func (v *Value) MustArray() []*Value {
	o, err := v.Array()
	if err != nil {
		panic(err)
	}
	return o
}

// Number returns a numeric value if the value type is number, or an error otherwise.
func (v *Value) Number() (float64, error) {
	if v.t == NumberType {
		return v.n, nil
	}
	return 0, fmt.Errorf("expect number type but got %v", v.t)
}

// MustNumber returns a numeric value or panics if the value type is not number.
func (v *Value) MustNumber() float64 {
	n, err := v.Number()
	if err != nil {
		panic(err)
	}
	return n
}

// String returns a string value if the value type is string, or an error otherwise.
func (v *Value) String() (string, error) {
	if v.t != StringType {
		return "", fmt.Errorf("expect string type but got %v", v.t)
	}
	return v.s, nil
}

// MustString returns a string value or panics if the value type is not string.
func (v *Value) MustString() string {
	str, err := v.String()
	if err != nil {
		panic(err)
	}
	return str
}

// Bool returns a boolean value if the value type is boolean, or an error otherwise.
func (v *Value) Bool() (bool, error) {
	if v.t != BoolType {
		return false, fmt.Errorf("expect boolean type but got %v", v.t)
	}
	return v.b, nil
}

// MustBool returns a boolean value or panics if the value type is not boolean.
func (v *Value) MustBool() bool {
	b, err := v.Bool()
	if err != nil {
		panic(err)
	}
	return b
}

// Get returns value by the given keys path, and a boolean value indicating
// whether the key at path exists.
//
// NB: array indexes may be represented as decimal numbers in keys.
func (v *Value) Get(keys ...string) (*Value, bool) {
	var found bool
	for _, key := range keys {
		switch v.t {
		case ObjectType:
			v, found = v.o.Get(key)
			if !found {
				return nil, false
			}
		case ArrayType:
			n, err := strconv.Atoi(key)
			if err != nil || n < 0 || n >= len(v.a) {
				return nil, false
			}
			v = v.a[n]
		default:
			return nil, false
		}
	}
	return v, true
}

// MarshalTo appends marshaled v to dst and returns the result.
func (v *Value) MarshalTo(dst []byte) ([]byte, error) {
	switch v.t {
	case ObjectType:
		return v.o.MarshalTo(dst)
	case ArrayType:
		dst = append(dst, '[')
		var err error
		for i, vv := range v.a {
			dst, err = vv.MarshalTo(dst)
			if err != nil {
				return nil, err
			}
			if i != len(v.a)-1 {
				dst = append(dst, ',')
			}
		}
		dst = append(dst, ']')
		return dst, nil
	case StringType:
		return strconv.AppendQuote(dst, v.s), nil
	case NumberType:
		if float64(int(v.n)) == v.n {
			return strconv.AppendInt(dst, int64(v.n), 10), nil
		}
		return strconv.AppendFloat(dst, v.n, 'f', -1, 64), nil
	case BoolType:
		if v.b {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil
	case NullType:
		return append(dst, "null"...), nil
	default:
		return nil, fmt.Errorf("unexpected value type: %v", v.t)
	}
}

func (v *Value) setObject(o Object) {
	v.t = ObjectType
	v.o = o
}

func (v *Value) setString(s string) {
	v.t = StringType
	v.s = s
}

func (v *Value) setNumber(n float64) {
	v.t = NumberType
	v.n = n
}

func (v *Value) setArray(a []*Value) {
	v.t = ArrayType
	v.a = a
}
