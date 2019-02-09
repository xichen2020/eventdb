package query

import (
	"testing"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"

	"github.com/stretchr/testify/require"
)

func TestUnorderedSingleKeyResultGroupMarshalJSON(t *testing.T) {
	results := calculation.ResultArray{
		calculation.NewCountResult(),
		calculation.NewAvgResult(),
		calculation.NewMaxStringResult(),
	}
	groups, err := NewSingleKeyResultGroups(field.StringType, results, nil, 10, 10)
	require.NoError(t, err)

	key1 := &field.ValueUnion{
		Type:      field.StringType,
		StringVal: "foo",
	}
	res, status := groups.GetOrInsertNoCheck(key1)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(32))
	res[2].Add(calculation.NewStringUnion("foo"))
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(16))
	res[2].Add(calculation.NewStringUnion("bar"))

	key2 := &field.ValueUnion{
		Type:      field.StringType,
		StringVal: "bar",
	}
	res, status = groups.GetOrInsertNoCheck(key2)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewStringUnion("baz"))
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewStringUnion("must"))

	b, err := groups.MarshalJSON(5, false)
	require.NoError(t, err)
	expectedVals := map[string]struct{}{
		`{"groups":[{"key":"foo","values":[2,24,"foo"]},{"key":"bar","values":[2,null,"must"]}]}`: struct{}{},
		`{"groups":[{"key":"bar","values":[2,null,"must"]},{"key":"foo","values":[2,24,"foo"]}]}`: struct{}{},
	}
	_, exists := expectedVals[string(b)]
	require.True(t, exists)
}

func TestOrderedSingleKeyResultGroupMarshalJSON(t *testing.T) {
	results := calculation.ResultArray{
		calculation.NewCountResult(),
		calculation.NewAvgResult(),
		calculation.NewMaxStringResult(),
	}
	orderBys := []OrderBy{
		{
			FieldType:  GroupByField,
			FieldIndex: 0,
			SortOrder:  Descending,
		},
		{
			FieldType:  CalculationField,
			FieldIndex: 2,
			SortOrder:  Ascending,
		},
	}
	groups, err := NewSingleKeyResultGroups(field.IntType, results, orderBys, 10, 10)
	require.NoError(t, err)

	// Add some data.
	key1 := &field.ValueUnion{
		Type:   field.IntType,
		IntVal: 10,
	}
	res, status := groups.GetOrInsertNoCheck(key1)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(32))
	res[2].Add(calculation.NewStringUnion("foo"))
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(16))
	res[2].Add(calculation.NewStringUnion("bar"))

	key2 := &field.ValueUnion{
		Type:   field.IntType,
		IntVal: 30,
	}
	res, status = groups.GetOrInsertNoCheck(key2)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewStringUnion("baz"))
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewStringUnion("must"))

	key3 := &field.ValueUnion{
		Type:   field.IntType,
		IntVal: 14,
	}
	res, status = groups.GetOrInsertNoCheck(key3)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewStringUnion("cat"))

	b, err := groups.MarshalJSON(5, true)
	require.NoError(t, err)
	expected := `{"groups":[{"key":30,"values":[2,null,"must"]},{"key":14,"values":[1,null,"cat"]},{"key":10,"values":[2,24,"foo"]}]}`
	require.Equal(t, expected, string(b))
}
