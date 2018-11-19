package query

import "github.com/xichen2020/eventdb/common"

// FilterOp represents a filter operation in string form.
type FilterOp string

// Filter operations.
const (
	FilterOpOr          FilterOp = "or"
	FilterOpAnd         FilterOp = "and"
	FilterOpEquals      FilterOp = "=="
	FilterOpNotEquals   FilterOp = "!="
	FilterOpContains    FilterOp = "in"
	FilterOpNotContains FilterOp = "not in"
)

// ValueType represents different types of values (if any) in a filter.
type ValueType string

// Value types.
const (
	ValueTypeNone   ValueType = ""
	ValueTypeString ValueType = "string"
	ValueTypeDouble ValueType = "double"
	ValueTypeBool   ValueType = "bool"
	ValueTypeInt    ValueType = "int"
)

// AggregationType represents a aggregation function to be applied on query results.
type AggregationType int

// Aggregation types.
const (
	AggregationTypeUnknown = iota
	AggregationTypeCount
)

// Filter is recursive struct that represents a query tree.
type Filter struct {
	LHS *Filter  `json:"lhs"`
	RHS *Filter  `json:"rhs"`
	Op  FilterOp `json:"op"`

	ValueType ValueType   `json:"valueType"`
	Value     interface{} `json:"value"`
}

// Query represents a query in json form
type Query struct {
	// These get converted to segment IDs
	Start    int64 `json:"start"`
	Stop     int64 `json:"stop"`
	Segments []common.SegmentID

	// only single filter per column for simplicity
	// map of column name -> filter
	Filters map[string]Filter `json:"filters"`

	// Only support a single aggregation for now
	Aggregation AggregationType `json:"aggregation"`
}
