package field

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewValuesLessThan(t *testing.T) {
	v1 := Values{
		{
			Type:      StringType,
			StringVal: "foo",
		},
		{
			Type:   IntType,
			IntVal: 123,
		},
		{
			Type:      DoubleType,
			DoubleVal: -12.3,
		},
	}

	v2 := Values{
		{
			Type:      StringType,
			StringVal: "foo",
		},
		{
			Type:   IntType,
			IntVal: 123,
		},
		{
			Type:      DoubleType,
			DoubleVal: -21.3,
		},
	}

	compareFns := []ValueCompareFn{
		MustCompareValue,
		MustReverseCompareValue,
		MustReverseCompareValue,
	}
	valuesLessThanFn := NewValuesLessThanFn(compareFns)
	require.True(t, valuesLessThanFn(v1, v2))
}

func TestFilterValues(t *testing.T) {
	v := Values{
		{
			Type:      StringType,
			StringVal: "foo",
		},
		{
			Type:   IntType,
			IntVal: 123,
		},
		{
			Type:      DoubleType,
			DoubleVal: -12.3,
		},
		{
			Type:   IntType,
			IntVal: 456,
		},
		{
			Type:      DoubleType,
			DoubleVal: -30.2,
		},
	}
	toExcludeIndices := []int{1, 4}
	filteredValues := FilterValues(v, toExcludeIndices)
	require.Equal(t, filteredValues, Values{v[0], v[2], v[3]})
}

func TestArrayBasedIterator(t *testing.T) {
	fields := []Field{
		{
			Path:  []string{"foo", "bar"},
			Value: NewIntUnion(123),
		},
		{
			Path:  []string{"baz"},
			Value: NewStringUnion("blah"),
		},
	}
	it := NewArrayBasedIterator(fields, nil)

	var actual []Field
	for it.Next() {
		actual = append(actual, it.Current())
	}
	require.Equal(t, fields, actual)
}
