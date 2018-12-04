package value

import (
	"fmt"
	"strconv"
)

// Value is a union of different types of values. There is at most one value
// in the union that is active at any point in time. The active value is
// determined by the type field in the value.
type Value struct {
	t Type
	b bool
	s string
	n float64
	a Array
	o Object
	p *Pool
}

// NewEmptyValue creates an empty value associated with the given pool.
func NewEmptyValue(p *Pool) *Value {
	return &Value{p: p}
}

// NewObjectValue creates a new object value.
func NewObjectValue(o Object, p *Pool) *Value {
	v := newValue(p)
	v.setObject(o)
	return v
}

// NewArrayValue creates a new array value.
func NewArrayValue(a Array, p *Pool) *Value {
	v := newValue(p)
	v.setArray(a)
	return v
}

// NewStringValue creates a new string value.
func NewStringValue(s string, p *Pool) *Value {
	v := newValue(p)
	v.setString(s)
	return v
}

// NewNumberValue creates a new number value.
func NewNumberValue(n float64, p *Pool) *Value {
	v := newValue(p)
	v.setNumber(n)
	return v
}

// NewBoolValue creates a new boolean value.
func NewBoolValue(b bool, p *Pool) *Value {
	v := newValue(p)
	v.setBool(b)
	return v
}

// NewNullValue creates a new null value.
func NewNullValue(p *Pool) *Value {
	v := newValue(p)
	v.setNull()
	return v
}

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
func (v *Value) Array() (Array, error) {
	if v.t != ArrayType {
		return Array{}, fmt.Errorf("expect array type but got %v", v.t)
	}
	return v.a, nil
}

// MustArray returns an array value or panics if the value type is not array.
func (v *Value) MustArray() Array {
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
			if err != nil || n < 0 || n >= len(v.a.raw) {
				return nil, false
			}
			v = v.a.raw[n]
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
		for i, vv := range v.a.raw {
			dst, err = vv.MarshalTo(dst)
			if err != nil {
				return nil, err
			}
			if i != len(v.a.raw)-1 {
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

// Reset resets the value.
func (v *Value) Reset() {
	p := v.p
	*v = Value{}
	v.p = p
}

// SetObject sets the value to an object value.
func (v *Value) SetObject(o Object) {
	v.Close()
	v.Reset()
	v.setObject(o)
}

// SetString sets the value to a string value.
func (v *Value) SetString(s string) {
	v.Close()
	v.Reset()
	v.setString(s)
}

// SetNumber sets the value to a numeric value.
func (v *Value) SetNumber(n float64) {
	v.Close()
	v.Reset()
	v.setNumber(n)
}

// SetArray sets the value to an array value.
func (v *Value) SetArray(a Array) {
	v.Close()
	v.Reset()
	v.setArray(a)
}

// SetBool sets the value to a boolean value.
func (v *Value) SetBool(b bool) {
	v.Close()
	v.Reset()
	v.setBool(b)
}

// SetNull sets the value to a null value.
func (v *Value) SetNull() {
	v.Close()
	v.Reset()
	v.setNull()
}

// Close closes the value and returns objects to pools where necessary.
func (v *Value) Close() {
	switch v.t {
	case ObjectType:
		v.o.Close()
	case ArrayType:
		v.a.Close()
	}
	p := v.p
	if p != nil {
		v.Reset()
		p.Put(v)
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

func (v *Value) setArray(a Array) {
	v.t = ArrayType
	v.a = a
}

func (v *Value) setBool(b bool) {
	v.t = BoolType
	v.b = b
}

func (v *Value) setNull() {
	v.t = NullType
}

func newValue(p *Pool) *Value {
	if p == nil {
		return &Value{}
	}
	return p.Get()
}

// Array is an array of values.
type Array struct {
	raw []*Value
	p   *BucketizedArrayPool
}

// NewArray creates a new value array.
func NewArray(raw []*Value, p *BucketizedArrayPool) Array {
	return Array{raw: raw, p: p}
}

// Raw returns the raw underlying value array.
func (a Array) Raw() []*Value { return a.raw }

// Capacity returns the capacity of the underlying array.
func (a Array) Capacity() int { return cap(a.raw) }

// Len returns the number of values.
func (a Array) Len() int { return len(a.raw) }

// Reset resets the value array.
func (a *Array) Reset() {
	for i := 0; i < len(a.raw); i++ {
		a.raw[i] = nil
	}
	a.raw = a.raw[:0]
}

// Append appends a value to the end of the value array.
func (a *Array) Append(v *Value) {
	if a.p == nil || len(a.raw) < cap(a.raw) {
		a.raw = append(a.raw, v)
		return
	}

	oldCapacity := cap(a.raw)
	oldArray := *a
	*a = oldArray.p.Get(oldCapacity * 2)
	n := copy(a.raw[:oldCapacity], oldArray.raw)
	oldArray.Reset()
	oldArray.p.Put(oldArray, oldCapacity)
	a.raw = append(a.raw[:n], v)
}

// Close closes the value array.
func (a Array) Close() {
	for i := range a.raw {
		a.raw[i].Close()
		a.raw[i] = nil
	}
	p := a.p
	if p != nil {
		a.Reset()
		p.Put(a, cap(a.raw))
	}
}
