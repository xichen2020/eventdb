package executor

import (
	"fmt"
	"sort"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/query"

	xerrors "github.com/m3db/m3x/errors"
)

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectRawResults(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	rawDocSourceField indexfield.BytesField,
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
// TODO(xichen): Handle this properly.
func collectUnorderedRawResults(
	rawDocSourceField indexfield.BytesField,
	maskingDocIDSetIt index.DocIDSetIterator,
	res *query.RawResults,
) error {
	// Return unordered results.
	rawDocSourceIter, err := rawDocSourceField.Fetch(maskingDocIDSetIt)
	if err != nil {
		maskingDocIDSetIt.Close()
		return fmt.Errorf("error fetching raw doc source data: %v", err)
	}
	defer rawDocSourceIter.Close()

	for rawDocSourceIter.Next() {
		v := rawDocSourceIter.Value().SafeBytes()
		res.AddUnordered(query.RawResult{Data: v})
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
	rawDocSourceField indexfield.BytesField,
	maskingDocIDSetIter index.DocIDSetIterator,
	q query.ParsedRawQuery,
	res *query.RawResults,
) error {
	filteredOrderByIter, orderByIter, err := createFilteredOrderByIterator(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		maskingDocIDSetIter,
		q,
		res,
	)
	if err != nil {
		maskingDocIDSetIter.Close()
		return err
	}
	// NB(xichen): This also closes order by iterator.
	defer filteredOrderByIter.Close()

	// NB: We currently first determine the doc ID set eligible based on orderBy criteria
	// and then fetch the corresponding raw doc source data, instead of iterating over
	// both the orderBy fields and the raw doc source field in sequence. This is because
	// the raw doc source field is typically much larger in dataset size and expensive to
	// decode. As such we restrict the amount of decoding we need to do by restricting the
	// doc ID set first.
	orderedRawResults, err := collectTopNRawResultDocIDOrderByValues(
		filteredOrderByIter,
		orderByIter,
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
) (
	*index.InAllDocIDSetIterator,
	*indexfield.MultiFieldIntersectIterator,
	error,
) {
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
			var multiErr xerrors.MultiError
			multiErr = multiErr.Add(err)
			multiErr = closeIterators(multiErr, fieldIters)
			return nil, nil, multiErr.FinalError()
		}
		if !hasOrderByFieldTypes {
			res.OrderByFieldTypes = append(res.OrderByFieldTypes, ft)
		}
		fieldIters = append(fieldIters, it)
		fieldIdx++
	}
	// NB(xichen): Worth optimizing for the single-field-orderBy case?
	multiFieldIter := indexfield.NewMultiFieldIntersectIterator(fieldIters)
	filteredDocIDSetIter := index.NewInAllDocIDSetIterator(maskingDocIDSetIter, multiFieldIter)
	return filteredDocIDSetIter, multiFieldIter, nil
}

// collectTopNRawResultDocIDOrderByValues returns the top N doc IDs and the order by field values
// based on the ordering criteria defined by `orderBy` as well as the query limit. The result doc
// ID values array are not sorted in any order.
// NB: `filteredOrderByIter` is closed at callsite.
func collectTopNRawResultDocIDOrderByValues(
	docIDIter *index.InAllDocIDSetIterator,
	orderByIter *indexfield.MultiFieldIntersectIterator,
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
	for docIDIter.Next() {
		// NB(xichen): This is only valid till the next iteration.
		orderByValues := orderByIter.Values()
		dv := docIDValues{
			DocID:  docIDIter.DocID(),
			Values: orderByValues,
		}
		topNDocIDValues.Add(dv, addDocIDValuesOpts)
	}
	if err := docIDIter.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over filtered order by items: %v", err)
	}
	return topNDocIDValues.RawData(), nil
}

// collectTopNRawResults collects the top N raw results from the raw doc source field
// based on the doc IDs and the ordering specified in `res`.
// TODO(xichen): Handle this properly.
func collectTopNRawResults(
	rawDocSourceField indexfield.BytesField,
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
		currRes := query.RawResult{
			Data:          rawDocSourceIter.Value().SafeBytes(),
			OrderByValues: orderByValues,
		}
		res.AddOrdered(currRes, query.RawResultAddOptions{})
	}
	if err := rawDocSourceIter.Err(); err != nil {
		return fmt.Errorf("error iterating over raw doc source field: %v", err)
	}
	return nil
}
