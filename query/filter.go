package query

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
)

// RawFilterList is a list of raw filters.
type RawFilterList struct {
	Filters          []RawFilter        `json:"filters"`
	FilterCombinator *filter.Combinator `json:"filter_combinator"`
}

// RawFilter represents a raw query filter.
type RawFilter struct {
	Field string      `json:"field"`
	Op    filter.Op   `json:"op"`
	Value interface{} `json:"value"`
}

// FilterList is a list of parsed filters.
type FilterList struct {
	Filters          []Filter
	FilterCombinator filter.Combinator
}

// Filter is a parsed filter.
// TODO(xichen): Handle range query.
type Filter struct {
	FieldPath []string
	Op        filter.Op
	Value     *field.ValueUnion
}

// AllowedFieldTypes returns a list of allowed field types given the filter
// operator and the RHS value if applicable.
func (f Filter) AllowedFieldTypes() (field.ValueTypeSet, error) {
	return f.Op.AllowedTypes(f.Value)
}
