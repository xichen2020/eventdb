package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawResultHeapSortInPlace(t *testing.T) {
	input := []RawResult{
		{
			Data:  "foo",
			DocID: 145,
		},
		{
			Data:  "bar",
			DocID: 34,
		},
		{
			Data:  "baz",
			DocID: 69,
		},
		{
			Data:  "cat",
			DocID: 254,
		},
	}
	lessThanFn := func(v1, v2 RawResult) bool { return v1.DocID < v2.DocID }
	h := NewRawResultHeap(0, lessThanFn)
	for _, r := range input {
		h.Push(r)
	}
	sortedResults := h.SortInPlace()
	expected := []RawResult{input[3], input[0], input[2], input[1]}
	require.Equal(t, expected, sortedResults)
}
