package query

import (
	"math"
	"time"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/hash"
)

// ParsedTimeBucketQuery represents a validated, sanitized time bucket query.
// Query results are a list of time buckets sorted by time in ascending order,
// each which contains the count of documents whose timestamps fall in the bucket.
type ParsedTimeBucketQuery struct {
	Namespace           string
	StartNanosInclusive int64
	EndNanosExclusive   int64
	Granularity         time.Duration
	Filters             []FilterList

	// Derived fields.
	FieldConstraints map[hash.Hash]FieldMeta // Field constraints inferred from query
}

func newParsedTimeBucketQuery(q *ParsedQuery) (ParsedTimeBucketQuery, error) {
	tbq := ParsedTimeBucketQuery{
		Namespace:           q.Namespace,
		StartNanosInclusive: q.StartTimeNanos,
		EndNanosExclusive:   q.EndTimeNanos,
		Granularity:         *q.TimeGranularity,
		Filters:             q.Filters,
	}
	if err := tbq.computeDerived(q.opts); err != nil {
		return ParsedTimeBucketQuery{}, err
	}
	return tbq, nil
}

// NewTimeBucketResults creates a new time bucket results from the parsed time bucket query.
func (q *ParsedTimeBucketQuery) NewTimeBucketResults() *TimeBucketResults {
	bucketSizeNanos := q.Granularity.Nanoseconds()
	// Round down start timestamp to a multiple of the bucket size.
	startInclusiveBucketNanos := q.StartNanosInclusive / bucketSizeNanos * bucketSizeNanos
	endExclusiveBucketNanos := int64(math.Ceil(float64(q.EndNanosExclusive)/float64(bucketSizeNanos))) * bucketSizeNanos
	numBuckets := int((endExclusiveBucketNanos - startInclusiveBucketNanos) / bucketSizeNanos)
	return &TimeBucketResults{
		StartBucketNanos: startInclusiveBucketNanos,
		BucketSizeNanos:  bucketSizeNanos,
		NumBuckets:       numBuckets,
	}
}

// NumFieldsForQuery returns the total number of fields involved in executing the query.
func (q *ParsedTimeBucketQuery) NumFieldsForQuery() int {
	numFieldsForQuery := 1 // Timestamp field
	numFieldsForQuery += q.NumFilters()
	return numFieldsForQuery
}

// NumFilters returns the number of filters in the query.
func (q *ParsedTimeBucketQuery) NumFilters() int {
	numFilters := 0
	for _, f := range q.Filters {
		numFilters += len(f.Filters)
	}
	return numFilters
}

func (q *ParsedTimeBucketQuery) computeDerived(opts ParseOptions) error {
	fieldConstraints, err := q.computeFieldConstraints(opts)
	if err != nil {
		return err
	}
	q.FieldConstraints = fieldConstraints
	return nil
}

func (q *ParsedTimeBucketQuery) computeFieldConstraints(
	opts ParseOptions,
) (map[hash.Hash]FieldMeta, error) {
	// Compute total number of fields involved in executing the query.
	numFieldsForQuery := q.NumFieldsForQuery()

	// Collect fields needed for query execution into a map for deduplciation.
	fieldMap := make(map[hash.Hash]FieldMeta, numFieldsForQuery)

	// Insert timestamp field.
	currIndex := 0
	addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
		FieldPath:  opts.TimestampFieldPath,
		IsRequired: true,
		AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.TimeType: struct{}{},
			},
		},
	})

	// Insert filter fields.
	currIndex++
	for _, fl := range q.Filters {
		for _, f := range fl.Filters {
			allowedFieldTypes, err := f.AllowedFieldTypes()
			if err != nil {
				return nil, err
			}
			addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
				FieldPath:  f.FieldPath,
				IsRequired: false,
				AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
					currIndex: allowedFieldTypes,
				},
			})
			currIndex++
		}
	}

	return fieldMap, nil
}
