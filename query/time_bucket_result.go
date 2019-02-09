package query

import (
	"encoding/json"
	"fmt"
)

// TimeBucketResults is a collection of time buckets recording the counts
// for documents falling in each bucket.
type TimeBucketResults struct {
	StartBucketNanos int64 // Inclusive
	BucketSizeNanos  int64
	NumBuckets       int

	// buckets records the document count in each bucket.
	buckets []int
}

// IsEmpty returns true if the result has not collected any values.
func (r *TimeBucketResults) IsEmpty() bool { return len(r.buckets) == 0 }

// Clear clears the grouped results.
func (r *TimeBucketResults) Clear() {
	r.buckets = nil
}

// AddAt adds a document at the given timestamp.
func (r *TimeBucketResults) AddAt(timestampNanos int64) {
	if r.buckets == nil {
		r.buckets = make([]int, r.NumBuckets)
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

// MarshalJSON marshals the time bucket results as a JSON object.
func (r *TimeBucketResults) MarshalJSON() ([]byte, error) {
	buckets := make([]timeBucketJSON, 0, len(r.buckets))
	for i := 0; i < len(r.buckets); i++ {
		bucket := timeBucketJSON{
			StartAtNanos: r.StartBucketNanos + r.BucketSizeNanos*int64(i),
			Value:        r.buckets[i],
		}
		buckets = append(buckets, bucket)
	}
	res := timeBucketResultsJSON{
		Granularity: r.BucketSizeNanos,
		Buckets:     buckets,
	}
	return json.Marshal(res)
}

type timeBucketJSON struct {
	StartAtNanos int64 `json:"startAtNanos"` // Start time of the bucket in nanoseconds
	Value        int   `json:"value"`        // Count
}

type timeBucketResultsJSON struct {
	Granularity int64            `json:"granularity"`
	Buckets     []timeBucketJSON `json:"buckets"`
}
