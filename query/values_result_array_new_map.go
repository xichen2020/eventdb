package query

import "github.com/xichen2020/eventdb/document/field"

// NewValuesResultArrayMap creates a new values result array map.
func NewValuesResultArrayMap(initialSize int) *ValuesResultArrayHash {
	return valuesResultArrayHashAlloc(valuesResultArrayHashOptions{
		hash: func(values field.Values) ValuesResultArrayHashHash {
			return ValuesResultArrayHashHash(values.Hash())
		},
		equals: func(v1, v2 field.Values) bool {
			return v1.Equal(v2)
		},
		copy: func(v field.Values) field.Values {
			return v.Clone()
		},
		finalize:    nil, // No op on key removal
		initialSize: initialSize,
	})
}
