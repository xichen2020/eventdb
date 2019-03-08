package query

import (
	"testing"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/stretchr/testify/require"
)

func TestUnorderedMultiKeyResultGroupsMarshalJSON(t *testing.T) {
	results := calculation.ResultArray{
		calculation.NewCountResult(),
		calculation.NewAvgResult(),
		calculation.NewMaxBytesResult(),
	}
	groups, err := NewMultiKeyResultGroups(results, nil, 10, 10)
	require.NoError(t, err)

	// Add some data.
	keys1 := []field.ValueUnion{
		field.NewIntUnion(12),
		field.NewBytesUnion(iterator.Bytes{
			Data: []byte("aa"),
		}),
	}
	res, status := groups.GetOrInsert(keys1)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(32))
	res[2].Add(calculation.NewBytesUnion([]byte("foo")))
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(16))
	res[2].Add(calculation.NewBytesUnion([]byte("bar")))

	keys2 := []field.ValueUnion{
		field.NewIntUnion(30),
		field.NewBytesUnion(iterator.Bytes{
			Data: []byte("cc"),
		}),
	}
	res, status = groups.GetOrInsert(keys2)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewBytesUnion([]byte("baz")))
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewBytesUnion([]byte("must")))

	b, err := groups.MarshalJSON(5, false)
	require.NoError(t, err)
	expectedVals := map[string]struct{}{
		`{"groups":[{"key":[12,"aa"],"values":[2,24,"foo"]},{"key":[30,"cc"],"values":[2,null,"must"]}]}`: struct{}{},
		`{"groups":[{"key":[30,"cc"],"values":[2,null,"must"]},{"key":[12,"aa"],"values":[2,24,"foo"]}]}`: struct{}{},
	}
	_, exists := expectedVals[string(b)]
	require.True(t, exists)
}

func TestOrderedMultiKeyResultGroupsMarshalJSON(t *testing.T) {
	results := calculation.ResultArray{
		calculation.NewCountResult(),
		calculation.NewAvgResult(),
		calculation.NewMaxBytesResult(),
	}
	orderBys := []OrderBy{
		{
			FieldType:  GroupByField,
			FieldIndex: 1,
			SortOrder:  Descending,
		},
		{
			FieldType:  CalculationField,
			FieldIndex: 2,
			SortOrder:  Ascending,
		},
	}
	groups, err := NewMultiKeyResultGroups(results, orderBys, 10, 10)
	require.NoError(t, err)

	// Add some data.
	keys1 := []field.ValueUnion{
		field.NewIntUnion(12),
		field.NewBytesUnion(iterator.Bytes{
			Data: []byte("aa"),
		}),
	}
	res, status := groups.GetOrInsert(keys1)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(32))
	res[2].Add(calculation.NewBytesUnion([]byte("foo")))
	res[0].Add(calculation.ValueUnion{})
	res[1].Add(calculation.NewNumberUnion(16))
	res[2].Add(calculation.NewBytesUnion([]byte("bar")))

	keys2 := []field.ValueUnion{
		field.NewIntUnion(30),
		field.NewBytesUnion(iterator.Bytes{
			Data: []byte("cc"),
		}),
	}
	res, status = groups.GetOrInsert(keys2)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewBytesUnion([]byte("baz")))
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewBytesUnion([]byte("must")))

	keys3 := []field.ValueUnion{
		field.NewIntUnion(12),
		field.NewBytesUnion(iterator.Bytes{
			Data: []byte("bb"),
		}),
	}
	res, status = groups.GetOrInsert(keys3)
	require.Equal(t, Inserted, status)
	res[0].Add(calculation.ValueUnion{})
	res[2].Add(calculation.NewBytesUnion([]byte("cat")))

	b, err := groups.MarshalJSON(5, true)
	require.NoError(t, err)
	expected := `{"groups":[{"key":[30,"cc"],"values":[2,null,"must"]},{"key":[12,"bb"],"values":[1,null,"cat"]},{"key":[12,"aa"],"values":[2,24,"foo"]}]}`
	require.Equal(t, expected, string(b))
}
