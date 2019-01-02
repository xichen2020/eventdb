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

// BoolFilter returns a bool filter if applicable, or an error otherwise.
func (f Filter) BoolFilter() (filter.BoolFilter, error) {
	return f.Op.BoolFilter(f.Value)
}

// IntFilter returns an int filter if applicable, or an error otherwise.
func (f Filter) IntFilter() (filter.IntFilter, error) {
	return f.Op.IntFilter(f.Value)
}

// DoubleFilter returns a double filter if applicable, or an error otherwise.
func (f Filter) DoubleFilter() (filter.DoubleFilter, error) {
	return f.Op.DoubleFilter(f.Value)
}

// StringFilter returns a string filter if applicable, or an error otherwise.
func (f Filter) StringFilter() (filter.StringFilter, error) {
	return f.Op.StringFilter(f.Value)
}

// TimeFilter returns a time filter if applicable, or an error otherwise.
func (f Filter) TimeFilter() (filter.TimeFilter, error) {
	return f.Op.TimeFilter(f.Value)
}
