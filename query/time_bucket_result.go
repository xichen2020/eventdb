package query

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
)

// TimeBucketResults is a collection of time buckets recording the counts
// for documents falling in each bucket.
type TimeBucketResults struct {
	StartBucketNanos int64 // Inclusive
	BucketSizeNanos  int64
	NumBuckets       int

	// buckets records the document count in each bucket.
	buckets []int64
}

// Len returns the number of time buckets allocated.
func (r *TimeBucketResults) Len() int { return len(r.buckets) }

// IsEmpty returns true if the result has not collected any values.
func (r *TimeBucketResults) IsEmpty() bool { return r.Len() == 0 }

// IsOrdered returns true if the results are kept in order.
func (r *TimeBucketResults) IsOrdered() bool { return false }

// HasOrderedFilter returns true if the results supports filtering ordered values.
// This is used to determine whether the result should be used to fast eliminate ineligible
// segments by filtering out those whose range fall outside the current result value range.
func (r *TimeBucketResults) HasOrderedFilter() bool { return false }

// LimitReached returns true if we have collected enough results.
func (r *TimeBucketResults) LimitReached() bool { return false }

// IsComplete returns true if the query result is complete and can be returned
// immediately without performing any further subqueries if any.
func (r *TimeBucketResults) IsComplete() bool { return false }

// MinOrderByValues returns the orderBy field values for the smallest result in
// the result collection if applicable. This is only called if `HasOrderedFilter`
// returns true.
func (r *TimeBucketResults) MinOrderByValues() field.Values {
	panic("not implemented")
}

// MaxOrderByValues returns the orderBy field values for the largest result in
// the result collection. This is only called if `HasOrderedFilter`
// returns true.
func (r *TimeBucketResults) MaxOrderByValues() field.Values {
	panic("not implemented")
}

// FieldValuesLessThanFn returns the function to compare two set of field values.
// This is only called if `HasOrderedFilter` returns true.
func (r *TimeBucketResults) FieldValuesLessThanFn() field.ValuesLessThanFn {
	panic("not implemented")
}

// Clear clears the grouped results.
func (r *TimeBucketResults) Clear() {
	r.buckets = nil
}

// AddAt adds a document at the given timestamp.
func (r *TimeBucketResults) AddAt(timestampNanos int64) {
	if r.buckets == nil {
		r.buckets = make([]int64, r.NumBuckets)
	}
	bucketIdx := (timestampNanos - r.StartBucketNanos) / r.BucketSizeNanos
	r.buckets[bucketIdx]++
}

// MergeInPlace merges the other results into the current results in place.
// The other results become invalid after the merge.
// Precondition: The current results and the other results are generated from
// the same query.
func (r *TimeBucketResults) MergeInPlace(other *TimeBucketResults) error {
	if other == nil || other.IsEmpty() {
		return nil
	}
	if r.IsEmpty() {
		*r = *other
		other.Clear()
		return nil
	}
	if r.StartBucketNanos != other.StartBucketNanos {
		return fmt.Errorf("merging two time bucket results with different start bucket nanos %d and %d", r.StartBucketNanos, other.StartBucketNanos)
	}
	if r.BucketSizeNanos != other.BucketSizeNanos {
		return fmt.Errorf("merging two time bucket results with different bucket sizes %d and %d", r.BucketSizeNanos, other.BucketSizeNanos)
	}
	if r.NumBuckets != other.NumBuckets {
		return fmt.Errorf("merging two time bucket results with different number of buckets %d and %d", r.NumBuckets, other.NumBuckets)
	}
	for i := 0; i < r.NumBuckets; i++ {
		r.buckets[i] += other.buckets[i]
	}
	other.Clear()
	return nil
}
