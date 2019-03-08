package query

import (
	"encoding/json"
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/bytes"

	"github.com/stretchr/testify/require"
)

func TestRawResultHeapSortInPlace(t *testing.T) {
	input := []RawResult{
		{
			Data: []byte("foo"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o1"),
				}),
			},
		},
		{
			Data: []byte("bar"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o3"),
				}),
			},
		},
		{
			Data: []byte("baz"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o4"),
				}),
			},
		},
		{
			Data: []byte("cat"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o2"),
				}),
			},
		},
	}
	valuesReverseLessThanFn := func(v1, v2 field.Values) bool {
		return bytes.GreaterThan(v1[0].BytesVal, v2[0].BytesVal)
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
				Data: []byte("foo"),
			},
			{
				Data: []byte("bar"),
			},
			{
				Data: []byte("baz"),
			},
		},
	}

	b, err := json.Marshal(input)
	require.NoError(t, err)
	// NB(bodu): Values are `[]byte`s in the form of base64 encoded strings.
	expected := `{"raw":["Zm9v","YmFy","YmF6"]}`
	require.Equal(t, expected, string(b))
}

func TestOrderedRawResultsMarshalJSON(t *testing.T) {
	input := []RawResult{
		{
			Data: []byte("foo"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o1"),
				}),
			},
		},
		{
			Data: []byte("bar"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o3"),
				}),
			},
		},
		{
			Data: []byte("baz"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o4"),
				}),
			},
		},
		{
			Data: []byte("cat"),
			OrderByValues: field.Values{
				field.NewBytesUnion(iterator.Bytes{
					Data: []byte("o2"),
				}),
			},
		},
	}
	valuesReverseLessThanFn := func(v1, v2 field.Values) bool {
		return bytes.GreaterThan(v1[0].BytesVal, v2[0].BytesVal)
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
	// NB(bodu): Values are `[]byte`s in the form of base64 encoded strings.
	expected := `{"raw":["Zm9v","Y2F0","YmFy","YmF6"]}`
	require.Equal(t, expected, string(b))
}
