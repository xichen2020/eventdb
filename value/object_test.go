package value

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObject(t *testing.T) {
	total := 0
	var f func(k string, v *Value)
	f = func(k string, v *Value) {
		switch v.Type() {
		case ObjectType:
			v.MustObject().Visit(f)
		case ArrayType:
			a := v.MustArray()
			for _, vv := range a.Raw() {
				f("", vv)
			}
		case StringType:
			s := v.MustString()
			total += len(s)
		case NumberType:
			n := v.MustNumber()
			total += int(n)
		case BoolType:
			n := v.MustBool()
			if n {
				total++
			}
		}
	}

	o := NewObject(NewKVArray([]KV{
		{k: "blah", v: NewStringValue("blah", nil)},
		{k: "bar", v: NewNumberValue(9.1, nil)},
		{k: "true", v: NewBoolValue(true)},
		{k: "false", v: NewBoolValue(false)},
	}, nil))
	a := NewArrayValue(NewArray([]*Value{
		NewNumberValue(123, nil),
		NewStringValue("aaaaa", nil),
	}, nil), nil)
	kvs := NewKVArray([]KV{
		{k: "foo", v: NewStringValue("foo", nil)},
		{k: "bar", v: NewNumberValue(4.5, nil)},
		{k: "baz", v: a},
		{k: "cat", v: NewObjectValue(o, nil)},
	}, nil)
	v := NewObject(kvs)
	require.Equal(t, 4, v.Len())

	v.Visit(f)
	require.Equal(t, 149, total)

	// Make sure the json remains valid after visiting all the items.
	marshalled, err := v.MarshalTo(nil)
	require.NoError(t, err)

	expected := `{"foo":"foo","bar":4.5,"baz":[123,"aaaaa"],"cat":{"blah":"blah","bar":9.1,"true":true,"false":false}}`
	require.Equal(t, expected, string(marshalled))
}
