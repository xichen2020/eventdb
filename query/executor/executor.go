package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
)

// FieldRetriever is responsible for retrieving fields from persistent storage.
type FieldRetriever interface {
	// Retrieve retrieves a list of fields.
	Retrieve(
		seg segment.ImmutableSegment,
		toRetrieve []persist.RetrieveFieldOptions,
	) ([]indexfield.DocsField, error)
}

// Executor executes different types of queries against the database.
type Executor interface {
	// ExecuteRaw executes a raw query and stores results in `res`.
	ExecuteRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
		seg segment.ImmutableSegment,
		retriever FieldRetriever,
		res *query.RawResults,
	) error

	// ExecuteGrouped executes a group-by query and stores results in `res`.
	ExecuteGrouped(
		ctx context.Context,
		q query.ParsedGroupedQuery,
		seg segment.ImmutableSegment,
		retriever FieldRetriever,
		res *query.GroupedResults,
	) error

	// ExecuteTimeBucket executes a time-bucket query and stores results in `res`.
	ExecuteTimeBucket(
		ctx context.Context,
		q query.ParsedTimeBucketQuery,
		seg segment.ImmutableSegment,
		retriever FieldRetriever,
		res *query.TimeBucketResults,
	) error
}

var (
	errNoTimestampField                 = errors.New("no timestamp field in the segment")
	errNoRawDocSourceField              = errors.New("no raw doc source field in the segment")
	errNoBytesValuesInRawDocSourceField = errors.New("no bytes values in raw doc source field")
)

type executor struct{}

// NewExecutor creates a new query executor.
func NewExecutor() Executor {
	return &executor{}
}

func (e *executor) ExecuteRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
	seg segment.ImmutableSegment,
	retriever FieldRetriever,
	res *query.RawResults,
) error {
	// Fast path if the limit indicates no results are needed.
	if res.IsComplete() {
		return nil
	}

	numDocs := seg.Metadata().NumDocs
	if numDocs == 0 {
		return nil
	}

	// Check if this segment has all orderBy fields as they are required,
	// and bail early otherwise.
	for _, ob := range q.OrderBy {
		if _, exists := seg.FieldAt(ob.FieldPath); !exists {
			return nil
		}
	}

	if res.HasOrderedFilter() && res.Len() > 0 && res.LimitReached() {
		// We only proceed to check if the orderBy values are in range if we can terminate
		// early. As such, the result needs to support ordered filtering and the result
		// set size should have reached the query limit.
		mayBeInRange, err := orderByValuesMaybeInRange(seg, q.OrderBy, res)
		if err != nil {
			return err
		}
		if !mayBeInRange {
			return nil
		}
	}

	// Identify the set of fields needed for query execution.
	allowedFieldTypes, fieldIndexMap, fieldsToCollect, err := extractFieldsForQuery(
		seg,
		q.NumFieldsForQuery(),
		q.FieldConstraints,
	)
	if err != nil {
		return err
	}

	// Collect all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Raw doc source field
	// - Fields in `filters` if applicable
	// - Fields in `orderBy` if applicable
	queryFields, err := collectFields(seg, retriever, fieldsToCollect)
	if err != nil {
		return err
	}
	defer func() {
		for i := range queryFields {
			if queryFields[i] != nil {
				queryFields[i].Close()
				queryFields[i] = nil
			}
		}
	}()

	// Apply filters to determine the doc ID set matching the filters.
	filteredDocIDIter, err := applyFilters(
		q.StartNanosInclusive, q.EndNanosExclusive, q.Filters, allowedFieldTypes,
		q.TimestampFieldIndex(), q.FilterStartIndex(), fieldIndexMap, queryFields, numDocs,
	)
	if err != nil {
		return err
	}

	queryRawDocSourceFieldIdx := fieldIndexMap[q.RawDocSourceFieldIndex()]
	if queryFields[queryRawDocSourceFieldIdx] == nil {
		return errNoRawDocSourceField
	}
	rawDocSourceField, ok := queryFields[queryRawDocSourceFieldIdx].BytesField()
	if !ok {
		return errNoBytesValuesInRawDocSourceField
	}

	return collectRawResults(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		rawDocSourceField,
		filteredDocIDIter,
		q,
		res,
	)
}

func (e *executor) ExecuteGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
	seg segment.ImmutableSegment,
	retriever FieldRetriever,
	res *query.GroupedResults,
) error {
	// Fast path if the limit indicates no results are needed.
	if res.IsComplete() {
		return nil
	}

	numDocs := seg.Metadata().NumDocs
	if numDocs == 0 {
		return nil
	}

	// Check if this segment has all groupBy fields as they are required, and bail early
	// otherwise.
	for _, gb := range q.GroupBy {
		if _, exists := seg.FieldAt(gb); !exists {
			return nil
		}
	}

	// Check if this segment has values for calculation, and bail early otherwise.
	hasValuesForCalc := false
	for _, calc := range q.Calculations {
		if !calc.Op.RequiresField() {
			// This means the calculation does not require a calculation field and is
			// purely based on the group by fields, which means the segment should be
			// queried for calculating this result.
			hasValuesForCalc = true
			break
		}
		if _, exists := seg.FieldAt(calc.FieldPath); exists {
			// If the field needed for calculation exists, it means the segment has values
			// for this calculation.
			hasValuesForCalc = true
			break
		}
	}
	if !hasValuesForCalc {
		return nil
	}

	// Identify the set of fields needed for query execution.
	allowedFieldTypes, fieldIndexMap, fieldsToCollect, err := extractFieldsForQuery(
		seg,
		q.NumFieldsForQuery(),
		q.FieldConstraints,
	)
	if err != nil {
		return err
	}

	// Collect all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Fields in `filters` if applicable
	// - Fields in `groupBy` if applicable
	// - Fields in `calculation` if applicable
	// Fields in `orderBy` should appear either in `groupBy` or `calculation` clauses
	// and thus are also covered.
	queryFields, err := collectFields(seg, retriever, fieldsToCollect)
	if err != nil {
		return err
	}
	defer func() {
		for i := range queryFields {
			if queryFields[i] != nil {
				queryFields[i].Close()
				queryFields[i] = nil
			}
		}
	}()

	// Apply filters to determine the doc ID set matching the filters.
	filteredDocIDIter, err := applyFilters(
		q.StartNanosInclusive, q.EndNanosExclusive, q.Filters, allowedFieldTypes,
		q.TimestampFieldIndex(), q.FilterStartIndex(), fieldIndexMap, queryFields, numDocs,
	)
	if err != nil {
		return err
	}

	return collectGroupedResults(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		filteredDocIDIter,
		q,
		res,
	)
}

func (e *executor) ExecuteTimeBucket(
	ctx context.Context,
	q query.ParsedTimeBucketQuery,
	seg segment.ImmutableSegment,
	retriever FieldRetriever,
	res *query.TimeBucketResults,
) error {
	numDocs := seg.Metadata().NumDocs
	if numDocs == 0 {
		return nil
	}

	// Identify the set of fields needed for query execution.
	allowedFieldTypes, fieldIndexMap, fieldsToCollect, err := extractFieldsForQuery(
		seg,
		q.NumFieldsForQuery(),
		q.FieldConstraints,
	)
	if err != nil {
		return err
	}

	// Collect all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Fields in `filters` if applicable
	queryFields, err := collectFields(seg, retriever, fieldsToCollect)
	if err != nil {
		return err
	}
	defer func() {
		for i := range queryFields {
			if queryFields[i] != nil {
				queryFields[i].Close()
				queryFields[i] = nil
			}
		}
	}()

	// Apply filters to determine the doc ID set matching the filters.
	filteredDocIDIter, err := applyFilters(
		q.StartNanosInclusive, q.EndNanosExclusive, q.Filters, allowedFieldTypes,
		q.TimestampFieldIndex(), q.FilterStartIndex(), fieldIndexMap, queryFields, numDocs,
	)
	if err != nil {
		return err
	}

	queryTimestampFieldIdx := fieldIndexMap[q.TimestampFieldIndex()]
	if queryFields[queryTimestampFieldIdx] == nil {
		return errNoTimestampField
	}
	timestampField, ok := queryFields[queryTimestampFieldIdx].TimeField()
	if !ok {
		return errNoTimeValuesInTimestampField
	}

	return collectTimeBucketResults(
		timestampField,
		filteredDocIDIter,
		res,
	)
}

// orderByValuesMaybeInRange checks if the orderBy field values in this segment fall
// within the range of orderBy field values of the existing results.
// Precondition: The orderBy fields are guaranteed to exist, and the result set is guaranteed
// to support ordered filter and has reached the query limit.
func orderByValuesMaybeInRange(
	seg segment.ImmutableSegment,
	orderBy []query.OrderBy,
	res *query.RawResults,
) (bool, error) {
	var (
		minOrderByValuesInResults = res.MinOrderByValues()
		maxOrderByValuesInResults = res.MaxOrderByValues()
		minOrderByValuesInSegment field.Values
		maxOrderByValuesInSegment field.Values
	)
	for i, ob := range orderBy {
		f, exists := seg.FieldAt(ob.FieldPath)
		if !exists {
			// This segment does not have one of the field to order results by,
			// as such we bail early as we require the orderBy field be present in
			// the result documents. This should never happen as part of the precondition
			// but just to be extra careful.
			return false, nil
		}
		availableTypes := f.Types()
		if len(availableTypes) > 1 {
			// We do not allow the orderBy field to have more than one type.
			return false, fmt.Errorf("order by field %v has multiple types %v", ob.FieldPath, availableTypes)
		}
		if minOrderByValuesInResults[i].Type != availableTypes[0] {
			// We expect the orderBy fields to have consistent types.
			return false, fmt.Errorf("order by field have type %v in the results and type %v in the segment", minOrderByValuesInResults[i].Type, availableTypes[0])
		}
		valueMetas := f.ValueMetas()
		minUnion, maxUnion, err := valueMetas[0].ToMinMaxValueUnion()
		if err != nil {
			return false, err
		}
		if minOrderByValuesInSegment == nil {
			minOrderByValuesInSegment = make(field.Values, 0, len(orderBy))
			maxOrderByValuesInSegment = make(field.Values, 0, len(orderBy))
		}
		if ob.SortOrder == query.Ascending {
			minOrderByValuesInSegment = append(minOrderByValuesInSegment, minUnion)
			maxOrderByValuesInSegment = append(maxOrderByValuesInSegment, maxUnion)
		} else {
			minOrderByValuesInSegment = append(minOrderByValuesInSegment, maxUnion)
			maxOrderByValuesInSegment = append(maxOrderByValuesInSegment, minUnion)
		}
	}

	valuesLessThanFn := res.FieldValuesLessThanFn()
	// Assert the values of all orderBy fields are within the bounds of existing results.
	if valuesLessThanFn(maxOrderByValuesInResults, minOrderByValuesInSegment) {
		// If the maximum existing result is less than the minimum raw result in this segment,
		// there is no need to query this segment as we've gathered enough raw results.
		return false, nil
	}
	if valuesLessThanFn(maxOrderByValuesInSegment, minOrderByValuesInResults) {
		// If the maximum raw result in this segment is less than the minimum existing result,
		// there is no need to query this segment as we've gathered enough raw results.
		return false, nil
	}
	return true, nil
}
