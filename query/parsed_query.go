package query

import (
	"fmt"
	"time"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/hash"
)

// Type is the type of a query.
type Type int

// A list of supported query types.
const (
	// RawQuery is a query that retrieves raw documents without grouping,
	// returning a list of raw documents with optional ordering applied.
	RawQuery Type = iota

	// GroupedQuery is a query that groups documents by fields and applies
	// calculations within each group, returning the grouped calculation results.
	GroupedQuery

	// TimeBucketQuery is a query that bucketizes the query time range into
	// time buckets and counts the number of documents falling into each bucket,
	// returning a list of counts ordered by time in ascending order.
	TimeBucketQuery
)

// ParsedQuery represents a validated, sanitized query object produced from a raw query.
type ParsedQuery struct {
	Namespace       string
	StartTimeNanos  int64
	EndTimeNanos    int64
	TimeGranularity *time.Duration
	Filters         []FilterList
	GroupBy         [][]string
	Calculations    []Calculation
	OrderBy         []OrderBy
	Limit           int

	// Derived fields for raw query.
	// AllowedFieldTypes map[hash.Hash]FieldMeta
	opts                    ParseOptions
	valuesLessThanFn        field.ValuesLessThanFn
	valuesReverseLessThanFn field.ValuesLessThanFn
}

// Type returns the query type.
func (q *ParsedQuery) Type() Type {
	if q.TimeGranularity != nil {
		return TimeBucketQuery
	}
	if len(q.GroupBy) == 0 {
		return RawQuery
	}
	return GroupedQuery
}

// RawQuery returns the parsed raw query.
func (q *ParsedQuery) RawQuery() (ParsedRawQuery, error) {
	return newParsedRawQuery(q)
}

// GroupedQuery returns the parsed grouped query.
func (q *ParsedQuery) GroupedQuery() (ParsedGroupedQuery, error) {
	return newParsedGroupedQuery(q)
}

// TimeBucketQuery returns the parsed time bucket query.
func (q *ParsedQuery) TimeBucketQuery() (ParsedTimeBucketQuery, error) {
	return newParsedTimeBucketQuery(q)
}

func (q *ParsedQuery) computeDerived() error {
	return q.computeValueCompareFns()
}

func (q *ParsedQuery) computeValueCompareFns() error {
	compareFns := make([]field.ValueCompareFn, 0, len(q.OrderBy))
	for _, ob := range q.OrderBy {
		compareFn, err := ob.SortOrder.CompareFieldValueFn()
		if err != nil {
			return fmt.Errorf("error determining the value compare fn for sort order %v: %v", ob.SortOrder, err)
		}
		compareFns = append(compareFns, compareFn)
	}
	q.valuesLessThanFn = field.NewValuesLessThanFn(compareFns)
	q.valuesReverseLessThanFn = func(v1, v2 field.Values) bool {
		return q.valuesLessThanFn(v2, v1)
	}
	return nil
}

// FieldMeta contains field metadata.
type FieldMeta struct {
	FieldPath []string

	// AllowedTypesBySourceIdx contains the set of field types allowed by the query,
	// keyed by the field index in the query clause.
	AllowedTypesBySourceIdx map[int]field.ValueTypeSet
}

// MergeInPlace merges the other field meta into the current field meta.
// Precondition: m.fieldPath == other.fieldPath.
// Precondition: The set of source indices in the two metas don't overlap.
func (m *FieldMeta) MergeInPlace(other FieldMeta) {
	for idx, types := range other.AllowedTypesBySourceIdx {
		m.AllowedTypesBySourceIdx[idx] = types
	}
}

func (q *ParsedGroupedQuery) computeNewCalculationResultArrayFn() calculation.NewResultArrayFromValueTypesFn {
	// Precondition: `fieldTypes` contains the field types for each calculation clause and has
	// the same size as `q.Calculations`. For calculation operators that do not require a field,
	// the corresponding item is an optional type whose `HasType` field is false.
	return func(fieldTypes field.OptionalTypeArray) (calculation.ResultArray, error) {
		results := make(calculation.ResultArray, 0, len(q.Calculations))
		for i, calc := range q.Calculations {
			var (
				res calculation.Result
				err error
			)
			if !calc.Op.RequiresField() {
				// Pass in an unknown type as the op does not require a field.
				res, err = calc.Op.NewResult(field.UnknownType)
			} else if fieldTypes[i].HasType {
				res, err = calc.Op.NewResult(fieldTypes[i].Type)
			}
			if err != nil {
				return nil, err
			}
			results = append(results, res)
		}
		return results, nil
	}
}

// addQueryFieldToMap adds a new query field meta to the existing
// field meta map.
func addQueryFieldToMap(
	fm map[hash.Hash]FieldMeta,
	fieldHashFn hash.StringArrayHashFn,
	newFieldMeta FieldMeta,
) {
	// Do not insert empty fields.
	if len(newFieldMeta.FieldPath) == 0 {
		return
	}
	fieldHash := fieldHashFn(newFieldMeta.FieldPath)
	meta, exists := fm[fieldHash]
	if !exists {
		fm[fieldHash] = newFieldMeta
		return
	}
	meta.MergeInPlace(newFieldMeta)
	fm[fieldHash] = meta
}

// Calculation represents a calculation object.
type Calculation struct {
	FieldPath []string
	Op        calculation.Op
}

// OrderBy is a field used for ordering results.
type OrderBy struct {
	FieldType  OrderByFieldType
	FieldIndex int
	FieldPath  []string
	SortOrder  SortOrder
}

// OrderByFieldType is the field type used for ordering.
type OrderByFieldType int

// A list of supported order-by field types.
const (
	UnknownOrderByFieldType OrderByFieldType = iota
	RawField
	GroupByField
	CalculationField
)
