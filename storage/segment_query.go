package storage

import (
	"fmt"
	"sort"

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
)

// validateOrderByClauses validates the fields and types specified in the query
// orderBy clauses are valid.
func validateOrderByClauses(
	allowedFieldTypes []field.ValueTypeSet,
	orderBy []query.OrderBy,
) (hasEmptyResult bool, err error) {
	orderByStart := len(allowedFieldTypes) - len(orderBy)
	for i := orderByStart; i < len(allowedFieldTypes); i++ {
		if len(allowedFieldTypes[i]) == 0 {
			// The field to order results by does not exist, as such we return an empty result early here.
			return true, nil
		}
		if len(allowedFieldTypes[i]) > 1 {
			// The field to order results by has more than one type. This is currently not supported.
			return false, fmt.Errorf("orderBy field %v has multiple types %v", orderBy[i-orderByStart], allowedFieldTypes[i])
		}
	}
	return false, nil
}

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
	timestampFieldIdx := fieldIndexMap[timestampFieldIdx]
	timestampField, exists := queryFields[timestampFieldIdx].TimeField()
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
func collectUnorderedRawDocSourceData(
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
		res.Add(query.RawResult{Data: rawDocSourceIter.Value()})
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
func collectOrderedRawDocSourceData(
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
		q.OrderBy,
	)
	if err != nil {
		// TODO(xichen): Add filteredDocIDIter.Err() here.
		maskingDocIDSetIter.Close()
		maskingDocIDSetIter = nil
		return err
	}
	defer filteredOrderByIter.Close()

	orderedRawResults, err := collectTopNRawResultDocIDOrderByValues(
		filteredOrderByIter,
		q.ResultReverseLessThanFn,
		q.Limit,
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
	orderBy []query.OrderBy,
) (*indexfield.DocIDMultiFieldIntersectIterator, error) {
	// Precondition: Each orderBy field has one and only one field type.
	var (
		orderByStart = len(allowedFieldTypes) - len(orderBy)
		orderByIters = make([]indexfield.BaseFieldIterator, 0, len(orderBy))
		err          error
	)
	for i := orderByStart; i < len(allowedFieldTypes); i++ {
		var t field.ValueType
		for key := range allowedFieldTypes[i] {
			t = key
			break
		}
		queryFieldIdx := fieldIndexMap[i]
		queryField := queryFields[queryFieldIdx]
		fu, found := queryField.FieldForType(t)
		if !found {
			err = fmt.Errorf("orderBy field %v does not have values of type %v", orderBy[i-orderByStart], t)
			break
		}
		var it indexfield.BaseFieldIterator
		it, err = fu.Iter()
		if err != nil {
			err = fmt.Errorf("error getting iterator for orderBy field %v type %v", orderBy[i-orderByStart], t)
			break
		}
		orderByIters = append(orderByIters, it)
	}

	if err != nil {
		// Clean up.
		var multiErr xerrors.MultiError
		for i := range orderByIters {
			if itErr := orderByIters[i].Err(); itErr != nil {
				multiErr = multiErr.Add(itErr)
			}
			orderByIters[i].Close()
			orderByIters[i] = nil
		}
		return nil, err
	}

	// NB(xichen): Worth optimizing for the single-field-orderBy case?

	orderByMultiIter := indexfield.NewMultiFieldIntersectIterator(orderByIters)
	filteredOrderByIter := indexfield.NewDocIDMultiFieldIntersectIterator(maskingDocIDSetIter, orderByMultiIter)
	return filteredOrderByIter, nil
}

// collectTopNRawResultDocIDOrderByValues returns the top N doc IDs and the field values to order
// raw results by based on the ordering criteria defined by `orderBy` as well as the query limit.
// The result array returned contains raw results ordered in the same order as that dictated by the
// `orderBy` clauses (e.g., if `orderBy` requires results by sorted by timestamp in descending order,
// the result array will also be sorted by timestamp in descending order). Note that the result array
// does not contain the actual raw result data, only the doc IDs and the orderBy field values.
func collectTopNRawResultDocIDOrderByValues(
	filteredOrderByIter *indexfield.DocIDMultiFieldIntersectIterator,
	lessThanFn query.RawResultLessThanFn,
	limit int,
) ([]query.RawResult, error) {
	// TODO(xichen): This algorithm runs in O(Nlogk) time. Should investigate whethere this is
	// in practice faster than first selecting top N values via a selection sort algorithm that
	// runs in O(N) time, then sorting the results in O(klogk) time.

	// Inserting values into the heap to select the top results based on the query ordering.
	// NB(xichen): The compare function has been reversed to keep the top N values. For example,
	// if we need to keep the top 10 values sorted in ascending order, it means we need the
	// 10 smallest values, and as such we keep a max heap and only add values to the heap
	// if the current value is smaller than the max heap value.
	results := query.NewRawResultHeap(limit, lessThanFn)
	for filteredOrderByIter.Next() {
		docID := filteredOrderByIter.DocID()
		values := filteredOrderByIter.Values() // values here is only valid till the next iteration.
		dv := query.RawResult{DocID: docID, OrderByValues: values}
		if results.Len() <= limit {
			// TODO(xichen): Should pool and reuse the value array here.
			valuesClone := make([]field.ValueUnion, len(values))
			copy(valuesClone, values)
			dv.OrderByValues = valuesClone
			results.Push(dv)
			continue
		}
		if min := results.Min(); !lessThanFn(min, dv) {
			continue
		}
		removed := results.Pop()
		// Reuse values array.
		copy(removed.OrderByValues, values)
		dv.OrderByValues = removed.OrderByValues
		results.Push(dv)
	}
	if err := filteredOrderByIter.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over filtered order by items: %v", err)
	}

	// Sort the result heap in place, and when done the items are sorted from left to
	// in the right order based on the query sorting criteria (i.e., if the sort order
	// is ascending, the leftmost item is the smallest item).
	for results.Len() > 0 {
		results.Pop()
	}
	return results.Data(), nil
}

// collectTopNRawResults collects the top N raw results from the raw doc source field
// based on the doc IDs and the ordering specified in `orderdRawResults`. The result
// array returned contains raw results ordered in the same order as that dictated by the
// `orderBy` clauses (e.g., if `orderBy` requires results by sorted by timestamp in descending order,
// the result array will also be sorted by timestamp in descending order).
func collectTopNRawResults(
	rawDocSourceField indexfield.StringField,
	orderedRawResults []query.RawResult,
	res *query.RawResults,
) error {
	for i := 0; i < len(orderedRawResults); i++ {
		orderedRawResults[i].OrderIdx = i
	}
	sort.Sort(query.RawResultsByDocIDAsc(orderedRawResults))
	docIDValuesIt := query.NewRawResultIterator(orderedRawResults)
	rawDocSourceIter, err := rawDocSourceField.Fetch(docIDValuesIt)
	if err != nil {
		return fmt.Errorf("error fetching raw doc source data: %v", err)
	}
	numItemsWithRawDocSource := 0
	for rawDocSourceIter.Next() {
		maskingPos := rawDocSourceIter.MaskingPosition()
		orderedRawResults[maskingPos].HasData = true
		orderedRawResults[maskingPos].Data = rawDocSourceIter.Value()
		numItemsWithRawDocSource++
	}
	if err := rawDocSourceIter.Err(); err != nil {
		return fmt.Errorf("error iterating over raw doc source field: %v", err)
	}
	sort.Sort(query.RawResultsByOrderIdxAsc(orderedRawResults))
	orderedRawResults = orderedRawResults[:numItemsWithRawDocSource]
	res.AddBatch(orderedRawResults)
	return nil
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
