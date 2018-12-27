package query

// ResultSet is a set of results.
type ResultSet struct {
	// Number of total raw events in the result set.
	numRawEvents int

	// Set of intermediate results that will be eventually merged to produce the
	// final result.
	// TODO(xichen): Change this to be a result iterator.
	results []Result
}

// LimitReached returns true if the result set size has reached the given limit.
// If the result is a raw query result, the limit applies to the number of raw events.
// NB: Since we only record the raw event count, this will always return false for
// group by queries. As a result, this does not affect functional correctness since
// this method is only used for early query termination.
func (rr ResultSet) LimitReached(limit *int) bool {
	if limit == nil {
		return false
	}
	if rr.numRawEvents >= *limit {
		return true
	}
	return false
}

// AddResult adds a result to the result set.
func (rr *ResultSet) AddResult(r Result) {
	if r.Raw != nil {
		rr.numRawEvents += len(r.Raw.Events)
	}
}

// AddResultSet adds a result set to the result set.
// TODO(xichen): This should just be creating multi-iterators.
func (rr *ResultSet) AddResultSet(r ResultSet) {
	numResults := rr.numRawEvents + r.numRawEvents
	if numResults > cap(rr.results) {
		results := make([]Result, 0, numResults)
		results = append(results, rr.results...)
		rr.results = results
	}
	rr.results = append(rr.results, r.results...)
	rr.numRawEvents += r.numRawEvents
}

// Finalize finalizes the results contained in the result set, and returns the
// final result computed from such set of results.
func (rr *ResultSet) Finalize() Result {
	panic("not implemented")
}

// Result contains the query result.
// The `Raw` field contains the raw event results, e.g., when no groupBy clause is specified
// in the query. The `Grouped` field contains the grouped results, e.g., when there is a
// group by clause in the query.
type Result struct {
	Raw     *RawResult     `json:"raw,omitempty"`
	Grouped *GroupedResult `json:"grouped,omitempty"`
}

// RawResult contains a list of raw event results.
// TODO(xichen): Represent the raw result set as iterators.
type RawResult struct {
	Events []string `json:"events"`
}

// AddRawResult adds a raw result into the current result.
// TODO(xichen): This should just be creating multi-iterators.
func (res *RawResult) AddRawResult(r RawResult) {
	numEvents := len(res.Events) + len(r.Events)
	if numEvents > cap(res.Events) {
		events := make([]string, 0, numEvents)
		events = append(events, res.Events...)
		res.Events = events
	}
	res.Events = append(res.Events, r.Events...)
}

// LimitReached returns true if the number of events contained in the raw result
// has reached the given limit.
func (res *RawResult) LimitReached(limit *int) bool {
	if limit == nil {
		return false
	}
	if len(res.Events) < *limit {
		return false
	}
	return true
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
