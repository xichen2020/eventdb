package value

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"

	"github.com/stretchr/testify/require"
)

func TestFieldIterator(t *testing.T) {
	v := NewObjectValue(
		NewObject(NewKVArray([]KV{
			{k: "xx", v: NewNumberValue(33.33, nil)},
			{k: "foo", v: NewArrayValue(NewArray([]*Value{
				NewNumberValue(123, nil),
				NewStringValue("bar", nil),
			}, nil), nil)},
			{k: "blah", v: NewObjectValue(NewObject(NewKVArray([]KV{
				{k: "bar", v: NewArrayValue(NewArray([]*Value{
					NewStringValue("baz", nil),
				}, nil), nil)},
				{k: "x", v: NewStringValue("y", nil)},
				{k: "duh", v: NewBoolValue(true, nil)},
				{k: "par", v: NewObjectValue(NewObject(NewKVArray([]KV{
					{k: "meh", v: NewNumberValue(3.0, nil)},
					{k: "got", v: NewNumberValue(4.5, nil)},
					{k: "are", v: NewNullValue(nil)},
				}, nil)), nil)},
			}, nil)), nil)},
			{k: "", v: NewStringValue("empty-key", nil)},
			{k: "empty-value", v: NewStringValue("", nil)},
		}, nil)), nil)

	expected := []field.Field{
		{
			Path:  []string{"xx"},
			Value: field.ValueUnion{Type: field.DoubleType, DoubleVal: 33.33},
		},
		{
			Path:  []string{"blah", "x"},
			Value: field.ValueUnion{Type: field.StringType, StringVal: "y"},
		},
		{
			Path:  []string{"blah", "duh"},
			Value: field.ValueUnion{Type: field.BoolType, BoolVal: true},
		},
		{
			Path:  []string{"blah", "par", "meh"},
			Value: field.ValueUnion{Type: field.IntType, IntVal: 3},
		},
		{
			Path:  []string{"blah", "par", "got"},
			Value: field.ValueUnion{Type: field.DoubleType, DoubleVal: 4.5},
		},
		{
			Path:  []string{"blah", "par", "are"},
			Value: field.ValueUnion{Type: field.NullType},
		},
		{
			Path:  []string{""},
			Value: field.ValueUnion{Type: field.StringType, StringVal: "empty-key"},
		},
		{
			Path:  []string{"empty-value"},
			Value: field.ValueUnion{Type: field.StringType, StringVal: ""},
		},
	}

	it := NewFieldIterator(v)
	defer it.Close()

	i := 0
	for it.Next() {
		f := it.Current()
		compareTestField(t, expected[i], f)
		i++
	}
	require.Equal(t, len(expected), i)
}

// Since field value is a union, the actual value may have irrelevant parts of
// the value set, which means we can't simply use `Equal` to compare the two values.
func compareTestField(t *testing.T, expected, actual field.Field) {
	require.Equal(t, expected.Path, actual.Path)
	require.Equal(t, expected.Value.Type, actual.Value.Type)
	switch expected.Value.Type {
	case field.NullType:
		return
	case field.BoolType:
		require.Equal(t, expected.Value.BoolVal, actual.Value.BoolVal)
	case field.IntType:
		require.Equal(t, expected.Value.IntVal, actual.Value.IntVal)
	case field.DoubleType:
		require.Equal(t, expected.Value.DoubleVal, actual.Value.DoubleVal)
	case field.StringType:
		require.Equal(t, expected.Value.StringVal, actual.Value.StringVal)
	default:
		require.Fail(t, "unexpected value type %v", expected.Value.Type)
	}
}
