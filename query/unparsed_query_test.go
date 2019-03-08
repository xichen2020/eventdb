package query

import (
	"encoding/json"
	"testing"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

func TestUnparsedGroupByQueryParse(t *testing.T) {
	input := `{
"namespace": "foo",
"end_time": 9876,
"time_range": "1h",
"filters": [
  {
    "filters": [
      {
        "field": "field1.field11",
        "op": "=",
        "value": "value1"
      },
      {
        "field": "field2",
        "op": ">",
        "value": "value2"
      }
    ],
    "filter_combinator": "AND"
  }
],
"group_by": [
  "field3.field31",
  "field4"
],
"calculations": [
  {
    "op": "COUNT"
  },
  {
    "op": "AVG",
    "field": "field5"
  }
],
"order_by": [
  {
    "field": "field3.field31",
    "order": "ascending"
  },
  {
    "field": "field5",
    "op": "AVG",
    "order": "descending"
  }
],
"limit": 10
}`

	var (
		valueUnion1 = field.NewBytesUnion(iterator.Bytes{
			Data: []byte("value1"),
		})
		valueUnion2 = field.NewBytesUnion(iterator.Bytes{
			Data: []byte("value2"),
		})
	)
	expected := ParsedQuery{
		Namespace:      "foo",
		StartTimeNanos: 6276000000000,
		EndTimeNanos:   9876000000000,
		Filters: []FilterList{
			{
				Filters: []Filter{
					{
						FieldPath: []string{"field1", "field11"},
						Op:        filter.Equals,
						Value:     &valueUnion1,
					},
					{
						FieldPath: []string{"field2"},
						Op:        filter.LargerThan,
						Value:     &valueUnion2,
					},
				},
				FilterCombinator: filter.And,
			},
		},
		GroupBy: [][]string{
			{"field3", "field31"},
			{"field4"},
		},
		Calculations: []Calculation{
			{
				Op: calculation.Count,
			},
			{
				Op:        calculation.Avg,
				FieldPath: []string{"field5"},
			},
		},
		OrderBy: []OrderBy{
			{
				FieldType:  GroupByField,
				FieldIndex: 0,
				FieldPath:  []string{"field3", "field31"},
				SortOrder:  Ascending,
			},
			{
				FieldType:  CalculationField,
				FieldIndex: 1,
				FieldPath:  []string{"field5"},
				SortOrder:  Descending,
			},
		},
		Limit: 10,
	}

	var p UnparsedQuery
	err := json.Unmarshal([]byte(input), &p)
	require.NoError(t, err)
	parsed, err := p.Parse(ParseOptions{
		FieldPathSeparator: byte('.'),
	})
	require.NoError(t, err)

	queryCmpOpts := cmpopts.IgnoreUnexported(ParsedQuery{})
	require.True(t, cmp.Equal(expected, parsed, queryCmpOpts))
}
