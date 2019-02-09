package query

import (
	"encoding/json"
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

func TestEmptyRawResultsMarshalJSON(t *testing.T) {
	input := &RawResults{}
	b, err := json.Marshal(input)
	require.NoError(t, err)
	expected := `{"raw":null}`
	require.Equal(t, expected, string(b))
}

func TestUnorderedRawResultsMarshalJSON(t *testing.T) {
	input := &RawResults{
		Unordered: []RawResult{
			{
				Data: "foo",
			},
			{
				Data: "bar",
			},
			{
				Data: "baz",
			},
		},
	}

	b, err := json.Marshal(input)
	require.NoError(t, err)
	expected := `{"raw":["foo","bar","baz"]}`
	require.Equal(t, expected, string(b))
}

func TestOrderedRawResultsMarshalJSON(t *testing.T) {
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
	h := NewTopNRawResults(4, rawResultLessThanFn)
	for _, r := range input {
		h.Add(r, RawResultAddOptions{})
	}
	res := &RawResults{
		OrderBy: []OrderBy{
			{
				FieldType: RawField,
				FieldPath: []string{"foo", "bar"},
			},
		},
		Ordered: h,
	}
	b, err := json.Marshal(res)
	require.NoError(t, err)
	expected := `{"raw":["foo","cat","bar","baz"]}`
	require.Equal(t, expected, string(b))
}
