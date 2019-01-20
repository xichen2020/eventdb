package query

import "github.com/xichen2020/eventdb/document/field"

// BaseResults represent query result collection.
type BaseResults interface {
	// Len returns the number of invidiual results in the result collection.
	Len() int

	// IsOrdered returns true if the results are ordered, and false otherwise.
	IsOrdered() bool

	// LimitReached returns true if the result collection has reached specified limit.
	LimitReached() bool

	// IsComplete returns true if the result collection is complete.
	IsComplete() bool

	// MinOrderByValues returns the orderBy field values for the smallest result in
	// the result collection.
	MinOrderByValues() []field.ValueUnion

	// MaxOrderByValues returns the orderBy field values for the largest result in
	// the result collection.
	MaxOrderByValues() []field.ValueUnion

	// FieldValuesLessThanFn returns the function to compare two set of field values.
	FieldValuesLessThanFn() field.ValuesLessThanFn
}
