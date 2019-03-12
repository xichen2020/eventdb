package executor

import (
	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/query"

	xerrors "github.com/m3db/m3x/errors"
)

const (
	// NB(xichen): Perhaps make this configurable.
	defaultInitResultGroupCapacity = 4096
)

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
