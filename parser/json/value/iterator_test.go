package value

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/bytes"

	"github.com/stretchr/testify/require"
)

func TestFieldIterator(t *testing.T) {
	v := NewObjectValue(
		NewObject(NewKVArray([]KV{
			{k: "xx", v: NewNumberValue(33.33, nil)},
			{k: "foo", v: NewArrayValue(NewArray([]*Value{
				NewNumberValue(123, nil),
				NewBytesValue([]byte("bar"), nil),
			}, nil), nil)},
			{k: "blah", v: NewObjectValue(NewObject(NewKVArray([]KV{
				{k: "bar", v: NewArrayValue(NewArray([]*Value{
					NewBytesValue([]byte("baz"), nil),
				}, nil), nil)},
				{k: "x", v: NewBytesValue([]byte("y"), nil)},
				{k: "duh", v: NewBoolValue(true, nil)},
				{k: "par", v: NewObjectValue(NewObject(NewKVArray([]KV{
					{k: "meh", v: NewNumberValue(3.0, nil)},
					{k: "got", v: NewNumberValue(4.5, nil)},
					{k: "are", v: NewNullValue(nil)},
				}, nil)), nil)},
			}, nil)), nil)},
			{k: "", v: NewBytesValue([]byte("empty-key"), nil)},
			{k: "empty-value", v: NewBytesValue([]byte(""), nil)},
		}, nil)), nil)

	expected := []field.Field{
		{
			Path:  []string{"xx"},
			Value: field.ValueUnion{Type: field.DoubleType, DoubleVal: 33.33},
		},
		{
			Path:  []string{"foo"},
			Value: field.ValueUnion{Type: field.ArrayType},
		},
		{
			Path:  []string{"blah", "bar"},
			Value: field.ValueUnion{Type: field.ArrayType},
		},
		{
			Path:  []string{"blah", "x"},
			Value: field.ValueUnion{Type: field.BytesType, BytesVal: bytes.NewImmutableBytes([]byte("y"))},
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
			Value: field.ValueUnion{Type: field.BytesType, BytesVal: bytes.NewImmutableBytes([]byte("empty-key"))},
		},
		{
			Path:  []string{"empty-value"},
			Value: field.ValueUnion{Type: field.BytesType, BytesVal: bytes.NewImmutableBytes([]byte(""))},
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
	case field.BytesType:
		require.Equal(t, expected.Value.BytesVal, actual.Value.BytesVal)
	case field.ArrayType:
	default:
		require.Fail(t, "unexpected value type %v", expected.Value.Type)
	}
}

func TestArrIterator(t *testing.T) {
	// During parsing, how do we know something is an array? we need to call it out in the payload
	nestedObj := NewObjectValue(
		NewObject(
			NewKVArray(
				[]KV{
					{k: "bar", v: NewArrayValue(NewArray([]*Value{
						NewBytesValue([]byte("baz"), nil),
					}, nil), nil)},
					{k: "x", v: NewBytesValue([]byte("y"), nil)},
					{k: "duh", v: NewBoolValue(true, nil)},
					{k: "par", v: NewObjectValue(
						NewObject(
							NewKVArray(
								[]KV{
									{k: "meh", v: NewNumberValue(3.0, nil)},
									{k: "got", v: NewNumberValue(4.5, nil)},
									{k: "are", v: NewNullValue(nil)},
								}, nil,
							),
						), nil,
					)},
				}, nil,
			),
		), nil,
	)

	v := NewObjectValue(
		NewObject(NewKVArray([]KV{
			{k: "xx", v: NewNumberValue(33.33, nil)},
			{k: "foo", v: NewArrayValue(
				NewArray(
					[]*Value{
						NewNumberValue(123, nil),
						NewBytesValue([]byte("bar"), nil),
						nestedObj,
					}, nil,
				), nil,
			)},
		}, nil)), nil)

	itt := NewFieldIterator(v)
	defer itt.Close()
	it := itt.(ArrayAwareIterator)

	it.Next()
	it.Next()
	require.Equal(t, field.ArrayType, it.Current().Value.Type)
	arr := it.Arr()

	expected := []Dump{
		{
			Path: []string{"foo"},
			Value: field.ValueUnion{
				Type:   field.IntType,
				IntVal: 123,
			},
		},
		{
			Path: []string{"foo"},
			Value: field.ValueUnion{
				Type:     field.BytesType,
				BytesVal: bytes.NewImmutableBytes([]byte("bar")),
			},
		},
		{
			Path: []string{"foo", "bar"},
			Value: field.ValueUnion{
				Type:     field.BytesType,
				BytesVal: bytes.NewImmutableBytes([]byte("baz")),
			},
		},
		{
			Path: []string{"foo", "x"},
			Value: field.ValueUnion{
				Type:     field.BytesType,
				BytesVal: bytes.NewImmutableBytes([]byte("y")),
			},
		},
		{
			Path: []string{"foo", "duh"},
			Value: field.ValueUnion{
				Type:    field.BoolType,
				BoolVal: true,
			},
		},
		{
			Path: []string{"foo", "par", "meh"},
			Value: field.ValueUnion{
				Type:   field.IntType,
				IntVal: 3,
			},
		},
		{
			Path: []string{"foo", "par", "got"},
			Value: field.ValueUnion{
				Type:      field.DoubleType,
				DoubleVal: 4.5,
			},
		},
		{
			Path: []string{"foo", "par", "are"},
			Value: field.ValueUnion{
				Type: field.NullType,
			},
		},
	}
	dumps := ArrayDump(it.Current().Path, arr)
	require.Equal(t, expected, dumps)
}
