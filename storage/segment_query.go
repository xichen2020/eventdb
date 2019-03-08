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
	"github.com/xichen2020/eventdb/values/iterator"
	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"

	xerrors "github.com/m3db/m3x/errors"
)

const (
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
	timestampFieldIdx int,
	filterStartIdx int,
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
	fieldIdx := filterStartIdx
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
	defer toFilter.Close()

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
		v := rawDocSourceIter.Value()
		switch v.Type {
		case iterator.DataTypeImmutable:
			// Immutable data is safe to reference between iterations.
			res.AddUnordered(query.RawResult{Data: v.Data})
		case iterator.DataTypeMutable:
			// TODO(bodu): These bytes should be pooled and returned to pool later when we're done w/ it.
			clone := make([]byte, len(v.Data))
			copy(clone, v.Data)
			res.AddUnordered(query.RawResult{Data: clone})
		default:
			return fmt.Errorf("error invalid raw doc source data type: %v", v.Type)
		}
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

		v := rawDocSourceIter.Value()
		currRes := query.RawResult{
			OrderByValues: orderByValues,
		}

		switch v.Type {
		case iterator.DataTypeImmutable:
			// Immutable data is safe to reference between iterations.
			currRes.Data = v.Data
		case iterator.DataTypeMutable:
			// TODO(bodu): These bytes should be pooled and returned to pool later when we're done w/ it.
			clone := make([]byte, len(v.Data))
			copy(clone, v.Data)
			currRes.Data = clone
		default:
			return fmt.Errorf("error invalid raw doc source data type: %v", v.Type)
		}

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
	filteredDocIDIter, groupByIter, calcIter, err := createFilteredGroupByCalcIterator(
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
	// NB(xichen): This also closes group by iterator and calculation iterator.
	defer filteredDocIDIter.Close()

	if len(res.GroupBy) == 1 {
		return collectSingleFieldGroupByResults(filteredDocIDIter, groupByIter, calcIter, res)
	}
	return collectMultiFieldGroupByResults(filteredDocIDIter, groupByIter, calcIter, res)
}

func createFilteredGroupByCalcIterator(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	maskingDocIDSetIter index.DocIDSetIterator,
	q query.ParsedGroupedQuery,
	res *query.GroupedResults,
) (
	*index.InAllDocIDSetIterator,
	*indexfield.MultiFieldIntersectIterator,
	*indexfield.MultiFieldUnionIterator,
	error,
) {
	// NB(xichen): For now we only allow each field in `GroupBy` and `Calculations`
	// to have a single type. As `OrderBy` fields refer to those either in `GroupBy`
	// or in `Calculations` clauses, they are also only allowed to have a single type.
	var (
		fieldIdx             = 1 + q.NumFilters() // Timestamp field followed by filter fields
		groupByIters         = make([]indexfield.BaseFieldIterator, 0, len(q.GroupBy))
		calcIters            = make([]indexfield.BaseFieldIterator, 0, len(q.Calculations))
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
			var multiErr xerrors.MultiError
			multiErr = multiErr.Add(err)
			multiErr = closeIterators(multiErr, groupByIters)
			return nil, nil, nil, multiErr.FinalError()
		}
		if !hasGroupByFieldTypes {
			res.GroupByFieldTypes = append(res.GroupByFieldTypes, ft)
		}
		groupByIters = append(groupByIters, it)
		fieldIdx++
	}

	if !hasCalcFieldTypes {
		res.CalcFieldTypes = make([]field.OptionalType, len(q.Calculations))
	}
	for i, calc := range q.Calculations {
		if calc.FieldPath == nil {
			continue
		}
		allowedTypes := allowedFieldTypes[fieldIdx]
		queryField := queryFields[fieldIndexMap[fieldIdx]]
		var expectedType *field.ValueType
		if res.CalcFieldTypes[i].HasType {
			expectedType = &res.CalcFieldTypes[i].Type
		}
		it, ft, err := newSingleTypeFieldIterator(calc.FieldPath, allowedTypes, queryField, expectedType)
		if err != nil {
			var multiErr xerrors.MultiError
			multiErr = multiErr.Add(err)
			multiErr = closeIterators(multiErr, groupByIters)
			multiErr = closeIterators(multiErr, calcIters)
			return nil, nil, nil, multiErr.FinalError()
		}
		if !res.CalcFieldTypes[i].HasType {
			res.CalcFieldTypes[i].HasType = true
			res.CalcFieldTypes[i].Type = ft
		}
		calcIters = append(calcIters, it)
		fieldIdx++
	}

	// We require all group by fields be present.
	groupByIter := indexfield.NewMultiFieldIntersectIterator(groupByIters)

	var (
		calcIter             *indexfield.MultiFieldUnionIterator
		filteredDocIDSetIter *index.InAllDocIDSetIterator
	)
	// Calculation fields are optional since we could have a COUNT op which requires no fields specified.
	if len(calcIters) > 0 {
		calcIter = indexfield.NewMultiFieldUnionIterator(calcIters)
		filteredDocIDSetIter = index.NewInAllDocIDSetIterator(maskingDocIDSetIter, groupByIter, calcIter)
	} else {
		filteredDocIDSetIter = index.NewInAllDocIDSetIterator(maskingDocIDSetIter, groupByIter)
	}
	return filteredDocIDSetIter, groupByIter, calcIter, nil
}

// Precondition: `res.CalcFieldTypes` contains the value type for each field that appear in the
// query calculation clauses, except those that do not require a field (e.g., `Count` calculations).
// NB: `groupByCalcIter` is closed at callsite.
func collectSingleFieldGroupByResults(
	docIDIter *index.InAllDocIDSetIterator,
	groupByIter *indexfield.MultiFieldIntersectIterator,
	calcIter *indexfield.MultiFieldUnionIterator, // Can be nil if there no ops that require a field.
	res *query.GroupedResults,
) error {
	var (
		toCalcValueFns []calculation.FieldValueToValueFn
		err            error
	)
	for docIDIter.Next() {
		// NB: Values become invalid at the next iteration.
		groupByVals := groupByIter.Values()
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
			initialized     bool
			calcVals        []field.ValueUnion
			hasVals         []bool
		)
		for i, calc := range res.Calculations {
			if !calc.Op.RequiresField() {
				calcResults[i].Add(emptyValueUnion)
				continue
			}
			if !initialized {
				// Precondition: len(calcVals) == number of calc fields.
				calcVals, hasVals = calcIter.Values()
				initialized = true
			}
			if !hasVals[calcFieldIdx] {
				// The calculation field does not have value for this iteration.
				calcFieldIdx++
				continue
			}
			cv := toCalcValueFns[i](&calcVals[calcFieldIdx])
			calcResults[i].Add(cv)
			calcFieldIdx++
		}
	}
	return docIDIter.Err()
}

// Precondition: `calcFieldTypes` contains the value type for each field that appear in the
// query calculation clauses in order, except those that do not require a field (e.g.,
// `Count` calculations).
// NB: `groupByCalcIter` is closed at callsite.
func collectMultiFieldGroupByResults(
	docIDIter *index.InAllDocIDSetIterator,
	groupByIter *indexfield.MultiFieldIntersectIterator,
	calcIter *indexfield.MultiFieldUnionIterator, // Can be nil if there no ops that require a field.
	res *query.GroupedResults,
) error {
	var (
		toCalcValueFns []calculation.FieldValueToValueFn
		err            error
	)
	for docIDIter.Next() {
		// NB: These values become invalid at the next iteration.
		groupByVals := groupByIter.Values()
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
			initialized     bool
			calcVals        []field.ValueUnion
			hasVals         []bool
		)
		for i, calc := range res.Calculations {
			if !calc.Op.RequiresField() {
				calcResults[i].Add(emptyValueUnion)
				continue
			}
			if !initialized {
				// Precondition: len(calcVals) == number of calc fields.
				calcVals, hasVals = calcIter.Values()
				initialized = true
			}
			if !hasVals[calcFieldIdx] {
				// The calculation field does not have value for this iteration.
				calcFieldIdx++
				continue
			}
			cv := toCalcValueFns[i](&calcVals[calcFieldIdx])
			calcResults[i].Add(cv)
			calcFieldIdx++
		}
	}
	return docIDIter.Err()
}

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectTimeBucketResults(
	timestampField indexfield.TimeField,
	maskingDocIDSetIter index.DocIDSetIterator,
	res *query.TimeBucketResults,
) error {
	timestampIter, err := timestampField.Fetch(maskingDocIDSetIter)
	if err != nil {
		maskingDocIDSetIter.Close()
		return fmt.Errorf("error fetching timestamp data: %v", err)
	}
	defer timestampIter.Close()

	// TODO(xichen): This decodes the timestamp field again if the timestamp field is loaded from
	// disk as we are currently not caching decoded values, and even if we do, we still do another
	// iteration of the timestamp values which we could have got from the filter iterators.
	for timestampIter.Next() {
		res.AddAt(timestampIter.Value())
	}
	if err := timestampIter.Err(); err != nil {
		return fmt.Errorf("error iterating over timestamp data: %v", err)
	}
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

func closeIterators(
	multiErr xerrors.MultiError,
	iters []indexfield.BaseFieldIterator,
) xerrors.MultiError {
	for i := range iters {
		if itErr := iters[i].Err(); itErr != nil {
			multiErr = multiErr.Add(itErr)
		}
		iters[i].Close()
		iters[i] = nil
	}
	return multiErr
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

// cloneDocIDValuesTo clones the (doc ID, values) pair from `src` to `target`.
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
