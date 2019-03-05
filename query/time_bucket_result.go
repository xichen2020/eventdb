package query

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/generated/proto/servicepb"
)

var (
	errNilTimeBucketQueryResultsProto = errors.New("nil time bucket query results proto")
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
	buckets := make([]TimeBucketQueryResult, 0, len(r.buckets))
	for i := 0; i < len(r.buckets); i++ {
		bucket := TimeBucketQueryResult{
			StartAtNanos: r.StartBucketNanos + r.BucketSizeNanos*int64(i),
			Value:        r.buckets[i],
		}
		buckets = append(buckets, bucket)
	}
	res := TimeBucketQueryResults{
		GranularityNanos: r.BucketSizeNanos,
		Buckets:          buckets,
	}
	return json.Marshal(res)
}

// ToProto converts the time bucket results to time bucket results proto message.
func (r *TimeBucketResults) ToProto() *servicepb.TimeBucketQueryResults {
	buckets := make([]servicepb.TimeBucketQueryResult, 0, len(r.buckets))
	for i := 0; i < len(r.buckets); i++ {
		bucket := servicepb.TimeBucketQueryResult{
			StartAtNanos: r.StartBucketNanos + r.BucketSizeNanos*int64(i),
			Value:        int64(r.buckets[i]),
		}
		buckets = append(buckets, bucket)
	}
	return &servicepb.TimeBucketQueryResults{
		GranularityNanos: r.BucketSizeNanos,
		Buckets:          buckets,
	}
}

// TimeBucketQueryResult contains the query result for a single time bucket.
// This is used to send results back to clients.
type TimeBucketQueryResult struct {
	StartAtNanos int64 `json:"startAtNanos"` // Start time of the bucket in nanoseconds
	Value        int   `json:"value"`        // Count
}

// TimeBucketQueryResults contains the results for a time bucket query.
type TimeBucketQueryResults struct {
	GranularityNanos int64                   `json:"granularity"`
	Buckets          []TimeBucketQueryResult `json:"buckets"`
}

// NewTimeBucketQueryResultsFromProto creates a new time bucket query results from
// corresponding protobuf message.
func NewTimeBucketQueryResultsFromProto(
	pbRes *servicepb.TimeBucketQueryResults,
) (*TimeBucketQueryResults, error) {
	if pbRes == nil {
		return nil, errNilTimeBucketQueryResultsProto
	}
	buckets := make([]TimeBucketQueryResult, 0, len(pbRes.Buckets))
	for _, pbBucket := range pbRes.Buckets {
		bucket := TimeBucketQueryResult{
			StartAtNanos: pbBucket.StartAtNanos,
			Value:        int(pbBucket.Value),
		}
		buckets = append(buckets, bucket)
	}
	return &TimeBucketQueryResults{
		GranularityNanos: pbRes.GranularityNanos,
		Buckets:          buckets,
	}, nil
}
