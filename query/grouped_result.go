package query

import "github.com/xichen2020/eventdb/document/field"

// TransformFn transforms a list of raw calculated values into a final calculated value.
// For example, computation of `Avg` should be transformed into computation of `Sum` and
// `Count`, and once the results are gathered, the final `Avg` value can be computed as
// `Sum` / `Count`.
type TransformFn func(rawValues []float64) float64

// CalculatedValue represents a calculated value. In simple cases this is simply a single
// numeric value. However, there are also cases where calculation of a value requires
// calculations of other intermediate values, which eventually get transformed into the
// final calculated value (e.g., the average value of a field).
type CalculatedValue struct {
	Value       float64
	Values      []float64
	TransformFn TransformFn
}

// ResultGroup is a result group.
type ResultGroup struct {
	// Group key corresponding to the `GroupBy` fields in the query.
	Keys []field.ValueUnion

	// Field values to order the groups by.
	OrderByValues []field.ValueUnion

	// A list of calculated values for a result group.
	CalculatedValues []CalculatedValue
}

// GroupedResults is a collection of result groups.
type GroupedResults struct {
	OrderBy            []OrderBy
	Limit              int
	ValuesLessThanFn   field.ValuesLessThanFn
	RequiredFieldPaths [][]string

	// If `OrderBy` is not empty, the groups are sorted in the order dictated by the `OrderBy`
	// clause in the query.
	Groups []ResultGroup
}

// Len returns the number of grouped results.
func (r *GroupedResults) Len() int { return len(r.Groups) }

// IsOrdered returns true if the grouped results are kept in order.
func (r *GroupedResults) IsOrdered() bool { return len(r.OrderBy) > 0 }

// LimitReached returns true if we have collected enough grouped results.
func (r *GroupedResults) LimitReached() bool { return r.Len() >= r.Limit }

// IsComplete returns true if the query result is complete and can be returned
// immediately without performing any further subqueries if any. This currently
// means the result should be unordered and the result collection size has reached
// the size limit. For ordered results, we should continue performing the subqueries
// if any since there may be future results that are ordered higher than the current results.
func (r *GroupedResults) IsComplete() bool { return r.LimitReached() && !r.IsOrdered() }

// MinOrderByValues returns the orderBy field values for the smallest result in
// the result collection.
func (r *GroupedResults) MinOrderByValues() []field.ValueUnion {
	if r.Len() == 0 {
		return nil
	}
	return r.Groups[0].OrderByValues
}

// MaxOrderByValues returns the orderBy field values for the largest result in
// the result collection.
func (r *GroupedResults) MaxOrderByValues() []field.ValueUnion {
	if r.Len() == 0 {
		return nil
	}
	return r.Groups[r.Len()-1].OrderByValues
}

// FieldValuesLessThanFn returns the function to compare two set of field values.
func (r *GroupedResults) FieldValuesLessThanFn() field.ValuesLessThanFn {
	return r.ValuesLessThanFn
}

// RequiredFields returns the field paths for required fields.
func (r *GroupedResults) RequiredFields() [][]string { return r.RequiredFieldPaths }

// Add adds a result group to the collection.
// For unordered grouped results:
// - If the results have not reached limit yet, the incoming result is appended at the end.
// - Otherwise, the incoming result is dropped.
// For ordered grouped results:
// - If the results have not reached limit yet, the incoming result is added in order.
// - Otherwise, the incoming result is inserted and the last result is dropped.
// TODO(xichen): Implement this.
func (r *GroupedResults) Add(rr ResultGroup) {
	panic("not implemented")
}

// AddBatch adds a batch of result groups to the collection.
// For unordered grouped results, the incoming batch is unsorted:
// - If the results have not reached limit yet, the incoming results are appended at the end
//   until the limit is reached, after which the incoming results are dropped.
// - Otherwise, the incoming results are dropped.
// For ordered grouped results, the incoming batch is sorted:
// - If the results have not reached limit yet, the incoming results are added in order
//   until the limit is reached, after which the incoming results are inserted and
//   the results beyond limit are dropped.
func (r *GroupedResults) AddBatch(rr []ResultGroup) {
	panic("not implemented")
}
