package value

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueTypeConversion(t *testing.T) {
	v := NewArrayValue(NewArray([]*Value{
		NewObjectValue(Object{}, nil),
		NewArrayValue(Array{}, nil),
		NewStringValue("", nil),
		NewNumberValue(123.45, nil),
		NewBoolValue(true, nil),
		NewNullValue(nil),
	}, nil), nil)
	a := v.MustArray().Raw()

	_, err := a[0].Object()
	require.NoError(t, err)
	_, err = a[0].Array()
	require.Error(t, err)

	_, err = a[1].Array()
	require.NoError(t, err)
	_, err = a[1].Object()
	require.Error(t, err)

	_, err = a[2].String()
	require.NoError(t, err)
	_, err = a[2].Number()
	require.Error(t, err)

	_, err = a[3].Number()
	require.NoError(t, err)
	_, err = a[3].String()
	require.Error(t, err)

	_, err = a[4].Bool()
	require.NoError(t, err)
	_, err = a[4].String()
	require.Error(t, err)

	_, err = a[5].Bool()
	require.Error(t, err)
}

func TestValueGet(t *testing.T) {
	v := NewObjectValue(NewObject(NewKVArray([]KV{
		{k: "xx", v: NewNumberValue(33.33, nil)},
		{k: "foo", v: NewArrayValue(NewArray([]*Value{
			NewNumberValue(123, nil),
			NewObjectValue(NewObject(NewKVArray([]KV{
				{k: "bar", v: NewArrayValue(NewArray([]*Value{NewStringValue("baz", nil)}, nil), nil)},
				{k: "x", v: NewStringValue("y", nil)},
			}, nil)), nil),
		}, nil), nil)},
		{k: "", v: NewStringValue("empty-key", nil)},
		{k: "empty-value", v: NewStringValue("", nil)},
	}, nil)), nil)

	sb, found := v.Get("")
	require.True(t, found)
	require.Equal(t, "empty-key", sb.MustString())
	sb, found = v.Get("empty-value")
	require.True(t, found)
	require.Equal(t, "", sb.MustString())

	vv, found := v.Get("foo", "1")
	require.True(t, found)
	o := vv.MustObject()
	n := 0
	o.Visit(func(k string, v *Value) {
		n++
		switch k {
		case "bar":
			require.Equal(t, ArrayType, v.Type())
			require.Equal(t, `["baz"]`, testMarshalled(t, v))
		case "x":
			require.Equal(t, "y", v.MustString())
		default:
			require.Failf(t, "unknown key: %s", k)
		}
	})
	require.Equal(t, 2, n)

	_, found = v.Get("nonexisting", "path")
	require.False(t, found)

	_, found = v.Get("foo", "bar", "baz")
	require.False(t, found)

	_, found = v.Get("foo", "-123")
	require.False(t, found)

	_, found = v.Get("foo", "234")
	require.False(t, found)

	_, found = v.Get("xx", "yy")
	require.False(t, found)
}

func testMarshalled(t *testing.T, v *Value) string {
	marshalled, err := v.MarshalTo(nil)
	require.NoError(t, err)
	return string(marshalled)
}
