package query

// Result contains the query result.
// The `Raw` field contains the raw event results, e.g., when no groupBy clause is specified
// in the query. The `Grouped` field contains the grouped results, e.g., when there is a
// group by clause in the query.
type Result struct {
	Raw     *RawResult     `json:"raw"`
	Grouped *GroupedResult `json:"grouped"`
}

// RawResult contains a list of raw event results.
type RawResult struct {
	Events []string `json:"events"`
}

// GroupedResult contains the results aggregated by different groups.
// The `Columns` specify the set of columns for each group, corresponding to
// the group keys for each result group. If a column is a log field, the column
// name is the field path. Otherwise if a column is an aggregation,
// `fieldpath_aggregation` is used as the column name.
type GroupedResult struct {
	Columns []string      `json:"columns"`
	Groups  []ResultGroup `json:"groups"`
}

// ResultGroup is a result group.
// NB: Only numeric fields and numeric aggregations are supported currently.
type ResultGroup struct {
	Key    []float64
	Values []float64
}
