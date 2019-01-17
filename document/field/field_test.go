package field

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewValuesLessThan(t *testing.T) {
	v1 := []ValueUnion{
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

	v2 := []ValueUnion{
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
		MustCompareUnion,
		MustReverseCompareUnion,
		MustReverseCompareUnion,
	}
	valuesLessThanFn := NewValuesLessThanFn(compareFns)
	require.True(t, valuesLessThanFn(v1, v2))
}

func TestFilterValues(t *testing.T) {
	v := []ValueUnion{
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
	require.Equal(t, filteredValues, []ValueUnion{v[0], v[2], v[3]})
}
