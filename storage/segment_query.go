package storage

import (
	"fmt"
	"sort"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/index"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/query"
	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"

	xerrors "github.com/m3db/m3x/errors"
)

const (
	timestampFieldIdx    = 0
	rawDocSourceFieldIdx = 1

	// NB(xichen): Perhaps make this configurable.
	defaultInitResultGroupCapacity = 4096
)

// applyFilters applies timestamp filters and other filters if applicable,
// and returns a doc ID iterator that outputs doc IDs matching the filtering criteria.
// TODO(xichen): Collapse filters against the same field.
func applyFilters(
	startNanosInclusive, endNanosExclusive int64,
	filters []query.FilterList,
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	numTotalDocs int32,
) (index.DocIDSetIterator, error) {
	queryTimestampFieldIdx := fieldIndexMap[timestampFieldIdx]
	timestampField, exists := queryFields[queryTimestampFieldIdx].TimeField()
	if !exists {
		return nil, errNoTimeValuesInTimestampField
	}

	// Fast path to compare min and max with query range.
	timestampFieldValues := timestampField.Values()
	timestampFieldMeta := timestampFieldValues.Metadata()
	if timestampFieldMeta.Min >= endNanosExclusive || timestampFieldMeta.Max < startNanosInclusive {
		return index.NewEmptyDocIDSetIterator(), nil
	}

	// Construct filtered time iterator.
	// TODO(xichen): Remove the logic to construct the iterator here once the range filter operator
	// is natively supported.
	docIDSetIter := timestampField.DocIDSet().Iter()
	timeIter, err := timestampFieldValues.Iter()
	if err != nil {
		return nil, err
	}
	timeRangeFilter := filter.NewTimeRangeFilter(startNanosInclusive, endNanosExclusive)
	positionIter := iterimpl.NewFilteredTimeIterator(timeIter, timeRangeFilter)
	filteredTimeIter := index.NewAtPositionDocIDSetIterator(docIDSetIter, positionIter)

	if len(filters) == 0 {
		return filteredTimeIter, nil
	}

	// Apply the remaining filters.
	allFilterIters := make([]index.DocIDSetIterator, 0, 1+len(filters))
	allFilterIters = append(allFilterIters, filteredTimeIter)
	fieldIdx := 2 // After timestamp and raw doc source
	for _, fl := range filters {
		var filterIter index.DocIDSetIterator
		if len(fl.Filters) == 1 {
			var (
				err           error
				allowedTypes  = allowedFieldTypes[fieldIdx]
				queryFieldIdx = fieldIndexMap[fieldIdx]
				queryField    = queryFields[queryFieldIdx]
			)
			filterIter, err = applyFilter(fl.Filters[0], queryField, allowedTypes, numTotalDocs)
			if err != nil {
				return nil, err
			}
			fieldIdx++
		} else {
			iters := make([]index.DocIDSetIterator, 0, len(fl.Filters))
			for _, f := range fl.Filters {
				var (
					allowedTypes  = allowedFieldTypes[fieldIdx]
					queryFieldIdx = fieldIndexMap[fieldIdx]
					queryField    = queryFields[queryFieldIdx]
				)
				iter, err := applyFilter(f, queryField, allowedTypes, numTotalDocs)
				if err != nil {
					return nil, err
				}
				iters = append(iters, iter)
				fieldIdx++
			}
			switch fl.FilterCombinator {
			case filter.And:
				filterIter = index.NewInAllDocIDSetIterator(iters...)
			case filter.Or:
				filterIter = index.NewInAnyDocIDSetIterator(iters...)
			default:
				return nil, fmt.Errorf("unknown filter combinator %v", fl.FilterCombinator)
			}
		}
		allFilterIters = append(allFilterIters, filterIter)
	}

	return index.NewInAllDocIDSetIterator(allFilterIters...), nil
}

// Precondition: The available field types in `fld` is a superset of in `fieldTypes`.
func applyFilter(
	flt query.Filter,
	fld indexfield.DocsField,
	fieldTypes field.ValueTypeSet,
	numTotalDocs int32,
) (index.DocIDSetIterator, error) {
	if fld == nil {
		// This means the field does not exist in this segment. Using a nil docs field allows us to
		// treat a nil docs field as a typed nil pointer, which handles the filtering logic the same
		// way as a valid docs field.
		return indexfield.NilDocsField.Filter(flt.Op, flt.Value, numTotalDocs)
	}

	// This restricts the docs field to apply filter against to only those in `fieldTypes`.
	toFilter, remainder, err := fld.NewDocsFieldFor(fieldTypes)
	if err != nil {
		return nil, err
	}
	defer fld.Close()

	if len(remainder) > 0 {
		return nil, fmt.Errorf("docs field types %v is not a superset of types %v to filter against", fld.Metadata().FieldTypes, fieldTypes)
	}
	return toFilter.Filter(flt.Op, flt.Value, numTotalDocs)
}

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectRawResults(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	rawDocSourceField indexfield.StringField,
	maskingDocIDSetIter index.DocIDSetIterator,
	q query.ParsedRawQuery,
	res *query.RawResults,
) error {
	if !res.IsOrdered() {
		return collectUnorderedRawResults(rawDocSourceField, maskingDocIDSetIter, res)
	}

	return collectOrderedRawResults(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		rawDocSourceField,
		maskingDocIDSetIter,
		q,
		res,
	)
}

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectUnorderedRawResults(
	rawDocSourceField indexfield.StringField,
	maskingDocIDSetIt index.DocIDSetIterator,
	res *query.RawResults,
) error {
	// Return unordered results
	rawDocSourceIter, err := rawDocSourceField.Fetch(maskingDocIDSetIt)
	if err != nil {
		maskingDocIDSetIt.Close()
		return fmt.Errorf("error fetching raw doc source data: %v", err)
	}
	defer rawDocSourceIter.Close()

	for rawDocSourceIter.Next() {
		res.AddUnordered(query.RawResult{Data: rawDocSourceIter.Value()})
		if res.LimitReached() {
			return nil
		}
	}
	if err := rawDocSourceIter.Err(); err != nil {
		return fmt.Errorf("error iterating over raw doc source data: %v", err)
	}
	return nil
}

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectOrderedRawResults(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	rawDocSourceField indexfield.StringField,
	maskingDocIDSetIter index.DocIDSetIterator,
	q query.ParsedRawQuery,
	res *query.RawResults,
) error {
	filteredOrderByIter, err := createFilteredOrderByIterator(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		maskingDocIDSetIter,
		q,
		res,
	)
	if err != nil {
		// TODO(xichen): Add filteredDocIDIter.Err() here.
		maskingDocIDSetIter.Close()
		maskingDocIDSetIter = nil
		return err
	}
	defer filteredOrderByIter.Close()

	// NB: We currently first determine the doc ID set eligible based on orderBy criteria
	// and then fetch the corresponding raw doc source data, instead of iterating over
	// both the orderBy fields and the raw doc source field in sequence. This is because
	// the raw doc source field is typically much larger in dataset size and expensive to
	// decode. As such we restrict the amount of decoding we need to do by restricting the
	// doc ID set first.
	orderedRawResults, err := collectTopNRawResultDocIDOrderByValues(
		filteredOrderByIter,
		q.ValuesReverseLessThanFn,
		res.Limit,
	)
	if err != nil {
		return err
	}

	return collectTopNRawResults(rawDocSourceField, orderedRawResults, res)
}

// NB: If an error is encountered, `maskingDocIDSetIter` should be closed at the callsite.
// Precondition: len(allowedFieldTypes) == 2 + number_of_filters + len(orderBy).
func createFilteredOrderByIterator(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	maskingDocIDSetIter index.DocIDSetIterator,
	q query.ParsedRawQuery,
	res *query.RawResults,
) (*indexfield.DocIDMultiFieldIntersectIterator, error) {
	var (
		fieldIdx             = 2 + q.NumFilters() // Timestamp field and raw doc source field followed by filter fields
		fieldIters           = make([]indexfield.BaseFieldIterator, 0, len(q.OrderBy))
		hasOrderByFieldTypes = len(res.OrderByFieldTypes) > 0
	)
	if !hasOrderByFieldTypes {
		res.OrderByFieldTypes = make([]field.ValueType, 0, len(q.OrderBy))
	}
	for i, ob := range q.OrderBy {
		allowedTypes := allowedFieldTypes[fieldIdx]
		queryField := queryFields[fieldIndexMap[fieldIdx]]
		var expectedType *field.ValueType
		if hasOrderByFieldTypes {
			// This should not escape.
			expectedType = &res.OrderByFieldTypes[i]
		}
		it, ft, err := newSingleTypeFieldIterator(ob.FieldPath, allowedTypes, queryField, expectedType)
		if err != nil {
			return nil, closeIteratorsOnError(fieldIters, err)
		}
		if !hasOrderByFieldTypes {
			res.OrderByFieldTypes = append(res.OrderByFieldTypes, ft)
		}
		fieldIters = append(fieldIters, it)
		fieldIdx++
	}
	// NB(xichen): Worth optimizing for the single-field-orderBy case?
	multiFieldIter := indexfield.NewMultiFieldIntersectIterator(fieldIters)
	filteredMultiFieldIter := indexfield.NewDocIDMultiFieldIntersectIterator(maskingDocIDSetIter, multiFieldIter)
	return filteredMultiFieldIter, nil
}

// collectTopNRawResultDocIDOrderByValues returns the top N doc IDs and the order by field values
// based on the ordering criteria defined by `orderBy` as well as the query limit. The result doc
// ID values array are not sorted in any order.
// NB: `filteredOrderByIter` is closed at callsite.
func collectTopNRawResultDocIDOrderByValues(
	filteredOrderByIter *indexfield.DocIDMultiFieldIntersectIterator,
	lessThanFn field.ValuesLessThanFn,
	limit int,
) ([]docIDValues, error) {
	// TODO(xichen): This algorithm runs in O(Nlogk) time. Should investigate whethere this is
	// in practice faster than first selecting top N values via a selection sort algorithm that
	// runs in O(N) time.

	// Inserting values into the heap to select the top results based on the query ordering.
	// NB(xichen): The compare function has been reversed to keep the top N values. For example,
	// if we need to keep the top 10 values sorted in ascending order, it means we need the
	// 10 smallest values, and as such we keep a max heap and only add values to the heap
	// if the current value is smaller than the max heap value.
	dvLessThanFn := newDocIDValuesLessThanFn(lessThanFn)
	topNDocIDValues := newTopNDocIDValues(limit, dvLessThanFn)
	addDocIDValuesOpts := docIDValuesAddOptions{
		CopyOnAdd: true,
		CopyFn:    cloneDocIDValues,
		CopyToFn:  cloneDocIDValuesTo,
	}
	for filteredOrderByIter.Next() {
		dv := docIDValues{
			DocID:  filteredOrderByIter.DocID(),
			Values: filteredOrderByIter.Values(), // values here is only valid till the next iteration
		}
		topNDocIDValues.Add(dv, addDocIDValuesOpts)
	}
	if err := filteredOrderByIter.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over filtered order by items: %v", err)
	}
	return topNDocIDValues.RawData(), nil
}

// collectTopNRawResults collects the top N raw results from the raw doc source field
// based on the doc IDs and the ordering specified in `res`.
func collectTopNRawResults(
	rawDocSourceField indexfield.StringField,
	topNDocIDValues []docIDValues, // Owned but not sorted in any order
	res *query.RawResults,
) error {
	// Fetching raw doc source data requires the doc IDs be sorted in ascending order.
	sort.Sort(docIDValuesByDocIDAsc(topNDocIDValues))

	docIDValuesIt := newDocIDValuesIterator(topNDocIDValues)
	rawDocSourceIter, err := rawDocSourceField.Fetch(docIDValuesIt)
	if err != nil {
		return fmt.Errorf("error fetching raw doc source data: %v", err)
	}
	for rawDocSourceIter.Next() {
		maskingPos := rawDocSourceIter.MaskingPosition() // Position in the (doc ID, values) sequence
		orderByValues := topNDocIDValues[maskingPos].Values
		currRes := query.RawResult{Data: rawDocSourceIter.Value(), OrderByValues: orderByValues}
		res.AddOrdered(currRes, query.RawResultAddOptions{})
	}
	if err := rawDocSourceIter.Err(); err != nil {
		return fmt.Errorf("error iterating over raw doc source field: %v", err)
	}
	return nil
}

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectGroupedResults(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	maskingDocIDSetIter index.DocIDSetIterator,
	q query.ParsedGroupedQuery,
	res *query.GroupedResults,
) error {
	filteredGroupByCalcIterator, err := createFilteredGroupByCalcIterator(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		maskingDocIDSetIter,
		q,
		res,
	)
	if err != nil {
		// TODO(xichen): Add filteredDocIDIter.Err() here.
		maskingDocIDSetIter.Close()
		maskingDocIDSetIter = nil
		return err
	}
	defer filteredGroupByCalcIterator.Close()

	if len(res.GroupBy) == 1 {
		return collectSingleFieldGroupByResults(filteredGroupByCalcIterator, res)
	}
	return collectMultiFieldGroupByResults(filteredGroupByCalcIterator, res)
}

func createFilteredGroupByCalcIterator(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	maskingDocIDSetIter index.DocIDSetIterator,
	q query.ParsedGroupedQuery,
	res *query.GroupedResults,
) (*indexfield.DocIDMultiFieldIntersectIterator, error) {
	// NB(xichen): For now we only allow each field in `GroupBy` and `Calculations`
	// to have a single type. As `OrderBy` fields refer to those either in `GroupBy`
	// or in `Calculations` clauses, they are also only allowed to have a single type.
	var (
		fieldIdx             = 1 + q.NumFilters() // Timestamp field followed by filter fields
		fieldIters           = make([]indexfield.BaseFieldIterator, 0, len(q.GroupBy)+len(q.Calculations))
		hasGroupByFieldTypes = len(res.GroupByFieldTypes) > 0
		hasCalcFieldTypes    = len(res.CalcFieldTypes) > 0
	)

	if !hasGroupByFieldTypes {
		res.GroupByFieldTypes = make([]field.ValueType, 0, len(q.GroupBy))
	}
	for i, gb := range q.GroupBy {
		allowedTypes := allowedFieldTypes[fieldIdx]
		queryField := queryFields[fieldIndexMap[fieldIdx]]
		var expectedType *field.ValueType
		if hasGroupByFieldTypes {
			expectedType = &res.GroupByFieldTypes[i]
		}
		it, ft, err := newSingleTypeFieldIterator(gb, allowedTypes, queryField, expectedType)
		if err != nil {
			return nil, closeIteratorsOnError(fieldIters, err)
		}
		if !hasGroupByFieldTypes {
			res.GroupByFieldTypes = append(res.GroupByFieldTypes, ft)
		}
		fieldIters = append(fieldIters, it)
		fieldIdx++
	}

	if !hasCalcFieldTypes {
		res.CalcFieldTypes = make([]field.ValueType, 0, len(q.Calculations))
	}
	for i, calc := range q.Calculations {
		if calc.FieldPath == nil {
			continue
		}
		allowedTypes := allowedFieldTypes[fieldIdx]
		queryField := queryFields[fieldIndexMap[fieldIdx]]
		var expectedType *field.ValueType
		if hasCalcFieldTypes {
			expectedType = &res.CalcFieldTypes[i]
		}
		it, ft, err := newSingleTypeFieldIterator(calc.FieldPath, allowedTypes, queryField, expectedType)
		if err != nil {
			return nil, closeIteratorsOnError(fieldIters, err)
		}
		if !hasCalcFieldTypes {
			res.CalcFieldTypes = append(res.CalcFieldTypes, ft)
		}
		fieldIters = append(fieldIters, it)
		fieldIdx++
	}

	multiFieldIter := indexfield.NewMultiFieldIntersectIterator(fieldIters)
	filteredMultiFieldIter := indexfield.NewDocIDMultiFieldIntersectIterator(maskingDocIDSetIter, multiFieldIter)
	return filteredMultiFieldIter, nil
}

// Precondition: `res.CalcFieldTypes` contains the value type for each field that appear in the
// query calculation clauses, except those that do not require a field (e.g., `Count` calculations).
// NB: `groupByCalcIter` is closed at callsite.
func collectSingleFieldGroupByResults(
	groupByCalcIter *indexfield.DocIDMultiFieldIntersectIterator,
	res *query.GroupedResults,
) error {
	var (
		toCalcValueFns []calculation.FieldValueToValueFn
		err            error
	)
	for groupByCalcIter.Next() {
		values := groupByCalcIter.Values()
		// NB: These values become invalid at the next iteration.
		groupByVals, calcVals := values[:1], values[1:]
		if res.SingleKeyGroups == nil {
			// `toCalcValueFns` has the same size as `calcFieldTypes`.
			toCalcValueFns, err = calculation.AsValueFns(res.CalcFieldTypes)
			if err != nil {
				return err
			}
			// `resultArray` has the same size as `q.Calculations`.
			resultArray, err := res.NewCalculationResultArrayFn(res.CalcFieldTypes)
			if err != nil {
				return err
			}
			res.SingleKeyGroups, err = query.NewSingleKeyResultGroups(
				groupByVals[0].Type,
				resultArray,
				res.OrderBy,
				res.NumGroupsLimit(),
				defaultInitResultGroupCapacity,
			)
			if err != nil {
				return err
			}
		}
		calcResults, status := res.SingleKeyGroups.GetOrInsertNoCheck(&groupByVals[0])
		if status == query.RejectedDueToLimit {
			// If this is a new group and we've reached limit, move on to next.
			continue
		}

		// Add values to calculation results.
		var (
			emptyValueUnion calculation.ValueUnion
			calcFieldIdx    int
		)
		for i, calc := range res.Calculations {
			if !calc.Op.RequiresField() {
				calcResults[i].Add(emptyValueUnion)
			} else {
				cv := toCalcValueFns[calcFieldIdx](&calcVals[calcFieldIdx])
				calcResults[i].Add(cv)
				calcFieldIdx++
			}
		}
	}
	return groupByCalcIter.Err()
}

// Precondition: `calcFieldTypes` contains the value type for each field that appear in the
// query calculation clauses in order, except those that do not require a field (e.g.,
// `Count` calculations).
// NB: `groupByCalcIter` is closed at callsite.
func collectMultiFieldGroupByResults(
	groupByCalcIter *indexfield.DocIDMultiFieldIntersectIterator,
	res *query.GroupedResults,
) error {
	var (
		numGroupByKeys = len(res.GroupBy)
		toCalcValueFns []calculation.FieldValueToValueFn
		err            error
	)
	for groupByCalcIter.Next() {
		values := groupByCalcIter.Values()
		// NB: These values become invalid at the next iteration.
		groupByVals, calcVals := values[:numGroupByKeys], values[numGroupByKeys:]
		if res.MultiKeyGroups == nil {
			// `toCalcValueFns` has the same size as `calcFieldTypes`.
			toCalcValueFns, err = calculation.AsValueFns(res.CalcFieldTypes)
			if err != nil {
				return err
			}
			// `resultArray` has the same size as `q.Calculations`.
			resultArray, err := res.NewCalculationResultArrayFn(res.CalcFieldTypes)
			if err != nil {
				return err
			}
			res.MultiKeyGroups, err = query.NewMultiKeyResultGroups(
				resultArray,
				res.OrderBy,
				res.NumGroupsLimit(),
				defaultInitResultGroupCapacity,
			)
			if err != nil {
				return err
			}
		}
		calcResults, status := res.MultiKeyGroups.GetOrInsert(groupByVals)
		if status == query.RejectedDueToLimit {
			// If this is a new group and we've reached limit, move on to next.
			continue
		}

		// Add values to calculation results.
		var (
			emptyValueUnion calculation.ValueUnion
			calcFieldIdx    int
		)
		for i, calc := range res.Calculations {
			if !calc.Op.RequiresField() {
				calcResults[i].Add(emptyValueUnion)
			} else {
				cv := toCalcValueFns[calcFieldIdx](&calcVals[calcFieldIdx])
				calcResults[i].Add(cv)
				calcFieldIdx++
			}
		}
	}
	return groupByCalcIter.Err()
}

func intersectFieldTypes(
	first []field.ValueType,
	second field.ValueTypeSet,
) field.ValueTypeSet {
	if len(first) == 0 || len(second) == 0 {
		return nil
	}
	res := make(field.ValueTypeSet, len(first))
	for _, t := range first {
		_, exists := second[t]
		if !exists {
			continue
		}
		res[t] = struct{}{}
	}
	return res
}

func newSingleTypeFieldIterator(
	fieldPath []string,
	allowedTypes field.ValueTypeSet,
	queryField indexfield.DocsField,
	expectedType *field.ValueType,
) (indexfield.BaseFieldIterator, field.ValueType, error) {
	if len(allowedTypes) != 1 {
		return nil, field.UnknownType, fmt.Errorf("field %s should only have one type but instead have types %v", fieldPath, allowedTypes)
	}
	var t field.ValueType
	for key := range allowedTypes {
		t = key
		break
	}
	if expectedType != nil && t != *expectedType {
		return nil, field.UnknownType, fmt.Errorf("field %s has type %v in the segment but expects type %v", fieldPath, t, *expectedType)
	}
	fu, found := queryField.FieldForType(t)
	if !found {
		return nil, field.UnknownType, fmt.Errorf("field %s does not have values of type %v", fieldPath, t)
	}
	it, err := fu.Iter()
	if err != nil {
		return nil, field.UnknownType, fmt.Errorf("error getting iterator for orderBy field %v type %v", fieldPath, t)
	}
	return it, t, nil
}

func closeIteratorsOnError(iters []indexfield.BaseFieldIterator, err error) error {
	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(err)
	for i := range iters {
		if itErr := iters[i].Err(); itErr != nil {
			multiErr = multiErr.Add(itErr)
		}
		iters[i].Close()
		iters[i] = nil
	}
	return multiErr.FinalError()
}

type docIDValues struct {
	DocID  int32
	Values field.Values
}

// cloneDocIDValues clones the incoming (doc ID, values) pair.
func cloneDocIDValues(v docIDValues) docIDValues {
	// TODO(xichen): Should pool and reuse the value array here.
	v.Values = v.Values.Clone()
	return v
}

// cloneDocIDValuesTo clonse the (doc ID, values) pair from `src` to `target`.
// Precondition: `target` values are guaranteed to have the same length as `src` and can be reused.
func cloneDocIDValuesTo(src docIDValues, target *docIDValues) {
	reusedValues := target.Values
	copy(reusedValues, src.Values)
	target.DocID = src.DocID
	target.Values = reusedValues
}

type docIDValuesByDocIDAsc []docIDValues

func (a docIDValuesByDocIDAsc) Len() int           { return len(a) }
func (a docIDValuesByDocIDAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a docIDValuesByDocIDAsc) Less(i, j int) bool { return a[i].DocID < a[j].DocID }

// docIDValuesIterator iterates over a sequence of (doc ID, values) pairs.
type docIDValuesIterator struct {
	data []docIDValues

	currIdx int
}

// newDocIDValuesIterator creates a new doc ID values iterator.
func newDocIDValuesIterator(data []docIDValues) *docIDValuesIterator {
	return &docIDValuesIterator{
		data:    data,
		currIdx: -1,
	}
}

// Next returns true if there are more pairs to be iterated over.
func (it *docIDValuesIterator) Next() bool {
	if it.currIdx >= len(it.data) {
		return false
	}
	it.currIdx++
	return it.currIdx < len(it.data)
}

// DocID returns the doc ID of the current pair.
func (it *docIDValuesIterator) DocID() int32 { return it.data[it.currIdx].DocID }

// Values returns the current collection of values.
func (it *docIDValuesIterator) Values() field.Values { return it.data[it.currIdx].Values }

// Err returns errors if any.
func (it *docIDValuesIterator) Err() error { return nil }

// Close closes the iterator.
func (it *docIDValuesIterator) Close() { it.data = nil }

type docIDValuesLessThanFn func(v1, v2 docIDValues) bool

func newDocIDValuesLessThanFn(fn field.ValuesLessThanFn) docIDValuesLessThanFn {
	return func(v1, v2 docIDValues) bool {
		return fn(v1.Values, v2.Values)
	}
}
