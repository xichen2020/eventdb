package query

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"

	"github.com/stretchr/testify/require"
)

func TestRawResultHeapSortInPlace(t *testing.T) {
	input := []RawResult{
		{
			Data: "foo",
			OrderByValues: field.Values{
				field.NewStringUnion("o1"),
			},
		},
		{
			Data: "bar",
			OrderByValues: field.Values{
				field.NewStringUnion("o3"),
			},
		},
		{
			Data: "baz",
			OrderByValues: field.Values{
				field.NewStringUnion("o4"),
			},
		},
		{
			Data: "cat",
			OrderByValues: field.Values{
				field.NewStringUnion("o2"),
			},
		},
	}
	valuesReverseLessThanFn := func(v1, v2 field.Values) bool {
		return v1[0].StringVal > v2[0].StringVal
	}
	rawResultLessThanFn := func(v1, v2 RawResult) bool {
		return valuesReverseLessThanFn(v1.OrderByValues, v2.OrderByValues)
	}
	h := NewTopNRawResults(2, rawResultLessThanFn)
	for _, r := range input {
		h.Add(r, RawResultAddOptions{})
	}
	sortedResults := h.SortInPlace()
	expected := []RawResult{input[0], input[3]}
	require.Equal(t, expected, sortedResults)
}
