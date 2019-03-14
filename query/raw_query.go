package query

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/hash"
)

// ParsedRawQuery represents a validated, sanitized raw query.
type ParsedRawQuery struct {
	Namespace               string
	StartNanosInclusive     int64
	EndNanosExclusive       int64
	Filters                 []FilterList
	OrderBy                 []OrderBy
	Limit                   int
	ValuesLessThanFn        field.ValuesLessThanFn
	ValuesReverseLessThanFn field.ValuesLessThanFn

	// Derived fields.
	ResultLessThanFn        RawResultLessThanFn
	ResultReverseLessThanFn RawResultLessThanFn
	FieldConstraints        map[hash.Hash]FieldMeta // Field constraints inferred from query
}

func newParsedRawQuery(q *ParsedQuery) (ParsedRawQuery, error) {
	rq := ParsedRawQuery{
		Namespace:               q.Namespace,
		StartNanosInclusive:     q.StartTimeNanos,
		EndNanosExclusive:       q.EndTimeNanos,
		Filters:                 q.Filters,
		OrderBy:                 q.OrderBy,
		Limit:                   q.Limit,
		ValuesLessThanFn:        q.valuesLessThanFn,
		ValuesReverseLessThanFn: q.valuesReverseLessThanFn,
	}
	if err := rq.computeDerived(q.opts); err != nil {
		return ParsedRawQuery{}, err
	}
	return rq, nil
}

// TimestampFieldIndex returns the index of the timestamp field.
func (q *ParsedRawQuery) TimestampFieldIndex() int { return 0 }

// RawDocSourceFieldIndex returns the index of the raw doc source field.
func (q *ParsedRawQuery) RawDocSourceFieldIndex() int { return 1 }

// FilterStartIndex returns the start index of fields in query filters if any.
func (q *ParsedRawQuery) FilterStartIndex() int { return 2 }

// NumFieldsForQuery returns the total number of fields involved in executing the query.
func (q *ParsedRawQuery) NumFieldsForQuery() int {
	numFieldsForQuery := 2 // Timestamp field and raw doc source field
	numFieldsForQuery += q.NumFilters()
	numFieldsForQuery += len(q.OrderBy)
	return numFieldsForQuery
}

// NumFilters returns the number of filters in the query.
func (q *ParsedRawQuery) NumFilters() int {
	numFilters := 0
	for _, f := range q.Filters {
		numFilters += len(f.Filters)
	}
	return numFilters
}

// NewRawResults creates a new raw results from the parsed raw query.
func (q *ParsedRawQuery) NewRawResults() *RawResults {
	return &RawResults{
		OrderBy:                 q.OrderBy,
		Limit:                   q.Limit,
		ValuesLessThanFn:        q.ValuesLessThanFn,
		ValuesReverseLessThanFn: q.ValuesReverseLessThanFn,
		ResultLessThanFn:        q.ResultLessThanFn,
		ResultReverseLessThanFn: q.ResultReverseLessThanFn,
	}
}

// computeDerived computes the derived fields.
func (q *ParsedRawQuery) computeDerived(opts ParseOptions) error {
	q.ResultLessThanFn = func(r1, r2 RawResult) bool {
		return q.ValuesLessThanFn(r1.OrderByValues, r2.OrderByValues)
	}
	q.ResultReverseLessThanFn = func(r1, r2 RawResult) bool {
		return q.ResultLessThanFn(r2, r1)
	}
	fieldConstraints, err := q.computeFieldConstraints(opts)
	if err != nil {
		return err
	}
	q.FieldConstraints = fieldConstraints
	return nil
}

func (q *ParsedRawQuery) computeFieldConstraints(
	opts ParseOptions,
) (map[hash.Hash]FieldMeta, error) {
	// Compute total number of fields involved in executing the query.
	numFieldsForQuery := q.NumFieldsForQuery()

	// Collect fields needed for query execution into a map for deduplciation.
	fieldMap := make(map[hash.Hash]FieldMeta, numFieldsForQuery)

	// Insert timestamp field.
	currIndex := 0
	addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
		FieldPath: opts.TimestampFieldPath,
		AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.TimeType: struct{}{},
			},
		},
	})

	// Insert raw doc source field.
	currIndex++
	addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
		FieldPath: opts.RawDocSourceFieldPath,
		AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.BytesType: struct{}{},
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
				FieldPath: f.FieldPath,
				AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
					currIndex: allowedFieldTypes,
				},
			})
			currIndex++
		}
	}

	// Insert order by fields.
	for _, ob := range q.OrderBy {
		addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
			FieldPath: ob.FieldPath,
			AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
				currIndex: field.OrderableTypes.Clone(),
			},
		})
		currIndex++
	}

	return fieldMap, nil
}
