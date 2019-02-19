package query

import (
	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/hash"
)

// ParsedGroupedQuery represents a validated, sanitized group query.
type ParsedGroupedQuery struct {
	Namespace           string
	StartNanosInclusive int64
	EndNanosExclusive   int64
	Filters             []FilterList
	GroupBy             [][]string
	Calculations        []Calculation
	OrderBy             []OrderBy
	Limit               int

	// Derived fields.
	NewCalculationResultArrayFn calculation.NewResultArrayFromValueTypesFn
	FieldConstraints            map[hash.Hash]FieldMeta // Field constraints inferred from query
}

func newParsedGroupedQuery(q *ParsedQuery) (ParsedGroupedQuery, error) {
	gq := ParsedGroupedQuery{
		Namespace:           q.Namespace,
		StartNanosInclusive: q.StartTimeNanos,
		EndNanosExclusive:   q.EndTimeNanos,
		Filters:             q.Filters,
		GroupBy:             q.GroupBy,
		Calculations:        q.Calculations,
		OrderBy:             q.OrderBy,
		Limit:               q.Limit,
	}
	if err := gq.computeDerived(q.opts); err != nil {
		return ParsedGroupedQuery{}, err
	}
	return gq, nil
}

// NewGroupedResults creates a new grouped results from the parsed grouped query.
func (q *ParsedGroupedQuery) NewGroupedResults() *GroupedResults {
	return &GroupedResults{
		GroupBy:                     q.GroupBy,
		Calculations:                q.Calculations,
		OrderBy:                     q.OrderBy,
		Limit:                       q.Limit,
		NewCalculationResultArrayFn: q.NewCalculationResultArrayFn,
	}
}

// TimestampFieldIndex returns the index of the timestamp field.
func (q *ParsedGroupedQuery) TimestampFieldIndex() int { return 0 }

// FilterStartIndex returns the start index of fields in query filters if any.
func (q *ParsedGroupedQuery) FilterStartIndex() int { return 1 }

// NumFieldsForQuery returns the total number of fields involved in executing the query.
// NB: `OrderBy` fields are covered by either `GroupBy` or `Calculations`
func (q *ParsedGroupedQuery) NumFieldsForQuery() int {
	numFieldsForQuery := 1 // Timestamp field
	numFieldsForQuery += q.NumFilters()
	numFieldsForQuery += len(q.GroupBy)
	for _, calc := range q.Calculations {
		if calc.FieldPath == nil {
			continue
		}
		numFieldsForQuery++
	}
	return numFieldsForQuery
}

// NumFilters returns the number of filters in the query.
func (q *ParsedGroupedQuery) NumFilters() int {
	numFilters := 0
	for _, f := range q.Filters {
		numFilters += len(f.Filters)
	}
	return numFilters
}

func (q *ParsedGroupedQuery) computeDerived(opts ParseOptions) error {
	fieldConstraints, err := q.computeFieldConstraints(opts)
	if err != nil {
		return err
	}
	q.FieldConstraints = fieldConstraints
	q.NewCalculationResultArrayFn = q.computeNewCalculationResultArrayFn()
	return nil
}

func (q *ParsedGroupedQuery) computeFieldConstraints(
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

	// Insert group by fields.
	for _, gb := range q.GroupBy {
		addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
			FieldPath: gb,
			AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
				currIndex: field.GroupableTypes.Clone(),
			},
		})
		currIndex++
	}

	// Insert calculation fields.
	for _, calc := range q.Calculations {
		if calc.FieldPath == nil {
			continue
		}
		allowedFieldTypes, err := calc.Op.AllowedTypes()
		if err != nil {
			return nil, err
		}
		// NB(xichen): Restrict the calculation field types for now, but in the future
		// can relax this constraint.
		addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
			FieldPath: calc.FieldPath,
			AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
				currIndex: allowedFieldTypes,
			},
		})
		currIndex++
	}

	return fieldMap, nil
}
