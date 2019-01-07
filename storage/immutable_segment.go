package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/index/generated/iterator"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/x/hash"

	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
)

type retrieveFieldOptions struct {
	fieldPath            []string
	fieldTypesToRetrieve field.ValueTypeSet
}

// immutableSegment is an immutable segment.
type immutableSegment interface {
	immutableSegmentBase

	// QueryRaw returns results for a given raw query.
	QueryRaw(
		ctx context.Context,
		startNanosInclusive, endNanosExclusive int64,
		filters []query.FilterList,
		orderBy []query.OrderBy,
		limit *int,
	) (query.RawResult, error)

	// LoadedStatus returns the segment loaded status.
	LoadedStatus() segmentLoadedStatus

	// Unload unloads all fields from memory.
	Unload() error

	// Flush flushes the segment to persistent storage.
	Flush(persistFns persist.Fns) error
}

var (
	errImmutableSegmentAlreadyClosed = errors.New("immutable segment is already closed")
	errNoTimeValuesInTimestampField  = errors.New("no time values in timestamp field")
	errFieldTypeNotFoundForFilter    = errors.New("the given field type is not found for filtering")
)

type segmentLoadedStatus int

const (
	segmentLoadedFullyInMem segmentLoadedStatus = iota
	segmentLoadedFromFS
	segmentUnloaded
)

type immutableSegmentOptions struct {
	timestampFieldPath    []string
	rawDocSourcePath      []string
	fieldHashFn           fieldHashFn
	retrieveFieldFromFSFn retrieveFieldFromFSFn
}

type immutableSeg struct {
	sync.RWMutex
	immutableSegmentBase

	timestampFieldPath    []string
	rawDocSourcePath      []string
	fieldHashFn           fieldHashFn
	retrieveFieldFromFSFn retrieveFieldFromFSFn

	closed  bool
	status  segmentLoadedStatus
	entries map[hash.Hash]*fieldEntry
}

type fieldEntry struct {
	fieldMeta index.DocsFieldMetadata
	field     index.DocsField
}

// nolint: unparam
func newImmutableSegment(
	id string,
	numDocs int32,
	minTimeNanos, maxTimeNanos int64,
	status segmentLoadedStatus,
	fields map[hash.Hash]index.DocsField,
	opts immutableSegmentOptions,
) *immutableSeg {
	entries := make(map[hash.Hash]*fieldEntry, len(fields))
	for k, f := range fields {
		entries[k] = &fieldEntry{
			fieldMeta: f.Metadata(),
			field:     f,
		}
	}
	return &immutableSeg{
		immutableSegmentBase:  newBaseSegment(id, numDocs, minTimeNanos, maxTimeNanos),
		timestampFieldPath:    opts.timestampFieldPath,
		rawDocSourcePath:      opts.rawDocSourcePath,
		fieldHashFn:           opts.fieldHashFn,
		retrieveFieldFromFSFn: opts.retrieveFieldFromFSFn,
		status:                status,
		entries:               entries,
	}
}

// How to execute the query.
// 1. Collect the set of fields that need to be involed in handling the query
//    - Timestamp field
//    - Raw docs field
//    - Fields in the filters list
//    - Fields used for ordering
// 2. Retrieve the aforementioned fields.
// 3. Filter for doc set IDs using timestamp filter, and the set of explicit filters if applicable,
//    stopping as soon as we hit the limit if applicable.
// 4. Retrieve raw doc source fields based on the set of doc IDs.
// 5. If we have order by clauses, retrieve those for the fields to sort raw docs against, then
//    order the raw doc values based on the sorting criteria.
func (s *immutableSeg) QueryRaw(
	ctx context.Context,
	startNanosInclusive, endNanosExclusive int64,
	filters []query.FilterList,
	orderBy []query.OrderBy,
	limit *int,
) (query.RawResult, error) {
	// Identify the set of fields needed for query execution.
	allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, err := s.collectFieldsForRawQuery(filters, orderBy)
	if err != nil {
		return query.RawResult{}, err
	}

	// Retrieve all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Raw doc source field
	// - Fields in `filters` if applicable
	// - Fields in `orderBy` if applicable
	queryFields, err := s.retrieveFields(fieldsToRetrieve)
	if err != nil {
		return query.RawResult{}, err
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
		startNanosInclusive, endNanosExclusive, filters,
		allowedFieldTypes, fieldIndexMap, queryFields, s.NumDocuments(),
	)
	if err != nil {
		return query.RawResult{}, err
	}

	// TODO(xichen): Finish the implementation here.
	_ = filteredDocIDIter
	return query.RawResult{}, errors.New("not implemented")
}

func (s *immutableSeg) LoadedStatus() segmentLoadedStatus {
	s.RLock()
	status := s.status
	s.RUnlock()
	return status
}

func (s *immutableSeg) Unload() error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errImmutableSegmentAlreadyClosed
	}
	if s.status == segmentUnloaded {
		return nil
	}
	s.status = segmentUnloaded
	// Nil out the field values but keep the field metadata so the segment
	// can easily determine whether a field needs to be loaded from disk.
	for _, entry := range s.entries {
		if entry.field != nil {
			entry.field.Close()
			entry.field = nil
		}
	}
	return nil
}

func (s *immutableSeg) Flush(persistFns persist.Fns) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errImmutableSegmentAlreadyClosed
	}

	// NB: Segment is only flushed once as a common case so it's okay to allocate
	// here for better readability than caching the buffer as a field.
	fieldBuf := make([]index.DocsField, 0, len(s.entries))
	for _, f := range s.entries {
		fieldBuf = append(fieldBuf, f.field.ShallowCopy())
	}
	s.RUnlock()

	err := persistFns.WriteFields(fieldBuf)

	// Close the docs field shallow copies.
	for i := range fieldBuf {
		fieldBuf[i].Close()
		fieldBuf[i] = nil
	}

	return err
}

func (s *immutableSeg) Close() {
	// Waiting for all readers to finish.
	s.immutableSegmentBase.Close()

	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	for _, entry := range s.entries {
		if entry.field != nil {
			entry.field.Close()
			entry.field = nil
		}
	}
	s.entries = nil
}

func (s *immutableSeg) collectFieldsForRawQuery(
	filters []query.FilterList,
	orderBy []query.OrderBy,
) (
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	fieldsToRetrieve []retrieveFieldOptions,
	err error,
) {
	// Compute total number of fields involved in executing the query.
	numFieldsForQuery := 2 // Timestamp field and raw doc source field
	for _, f := range filters {
		numFieldsForQuery += len(f.Filters)
	}
	numFieldsForQuery += len(orderBy)

	// Collect fields needed for query execution into a map for deduplciation.
	fieldMap := make(map[hash.Hash]queryFieldMeta, numFieldsForQuery)

	// Insert timestamp field.
	currIndex := 0
	s.addQueryFieldToMap(fieldMap, queryFieldMeta{
		fieldPath: s.timestampFieldPath,
		allowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.TimeType: struct{}{},
			},
		},
	})

	// Insert raw doc source field.
	currIndex++
	s.addQueryFieldToMap(fieldMap, queryFieldMeta{
		fieldPath: s.rawDocSourcePath,
		allowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.StringType: struct{}{},
			},
		},
	})

	// Insert filter fields.
	currIndex++
	for _, fl := range filters {
		for _, f := range fl.Filters {
			allowedFieldTypes, err := f.AllowedFieldTypes()
			if err != nil {
				return nil, nil, nil, err
			}
			s.addQueryFieldToMap(fieldMap, queryFieldMeta{
				fieldPath: f.FieldPath,
				allowedTypesBySourceIdx: map[int]field.ValueTypeSet{
					currIndex: allowedFieldTypes,
				},
			})
			currIndex++
		}
	}

	// Insert order by fields.
	for _, ob := range orderBy {
		s.addQueryFieldToMap(fieldMap, queryFieldMeta{
			fieldPath: ob.FieldPath,
			allowedTypesBySourceIdx: map[int]field.ValueTypeSet{
				currIndex: field.OrderableTypes.Clone(),
			},
		})
		currIndex++
	}

	// Intersect the allowed field types determined from the given query
	// with the available field types in the segment.
	s.intersectWithAvailableTypes(fieldMap)

	// Flatten the list of fields.
	allowedFieldTypes = make([]field.ValueTypeSet, numFieldsForQuery)
	fieldIndexMap = make([]int, numFieldsForQuery)
	fieldsToRetrieve = make([]retrieveFieldOptions, 0, len(fieldMap))
	fieldIndex := 0
	for _, f := range fieldMap {
		allAllowedTypes := make(field.ValueTypeSet)
		for sourceIdx, types := range f.allowedTypesBySourceIdx {
			allowedFieldTypes[sourceIdx] = types
			fieldIndexMap[sourceIdx] = fieldIndex
			for t := range types {
				allAllowedTypes[t] = struct{}{}
			}
		}
		fieldOpts := retrieveFieldOptions{
			fieldPath:            f.fieldPath,
			fieldTypesToRetrieve: allAllowedTypes,
		}
		fieldsToRetrieve = append(fieldsToRetrieve, fieldOpts)
		fieldIndex++
	}
	return allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, nil
}

// addQueryFieldToMap adds a new query field meta to the existing
// field meta map.
func (s *immutableSeg) addQueryFieldToMap(
	fm map[hash.Hash]queryFieldMeta,
	newFieldMeta queryFieldMeta,
) {
	// Do not insert empty fields.
	if len(newFieldMeta.fieldPath) == 0 {
		return
	}
	fieldHash := s.fieldHashFn(newFieldMeta.fieldPath)
	meta, exists := fm[fieldHash]
	if !exists {
		fm[fieldHash] = newFieldMeta
		return
	}
	meta.MergeInPlace(newFieldMeta)
	fm[fieldHash] = meta
}

// NB(xichen): The field path and types in the `entries` map don't change except
// when being closed. If we are here, it means there is an active query in which
// case `Close` will block until the query is done, and as a result there is no
// need to RLock here.
func (s *immutableSeg) intersectWithAvailableTypes(
	fm map[hash.Hash]queryFieldMeta,
) {
	for k, meta := range fm {
		var availableTypes []field.ValueType
		entry, exists := s.entries[k]
		if exists {
			availableTypes = entry.fieldMeta.FieldTypes
		}
		for srcIdx, allowedTypes := range meta.allowedTypesBySourceIdx {
			intersectedTypes := intersectFieldTypes(availableTypes, allowedTypes)
			meta.allowedTypesBySourceIdx[srcIdx] = intersectedTypes
		}
		fm[k] = meta
	}
}

// retrieveFields returns the set of field for a list of field retrieving options.
// If no error is returned, the result array contains the same number of slots
// as the number of fields to retrieve. Fields that don't exist in the segment
// will have a nil slot.
func (s *immutableSeg) retrieveFields(
	fields []retrieveFieldOptions,
) ([]index.DocsField, error) {
	// Gather fields that are already loaded and identify fields that need to be retrieved
	// from filesystem.
	// NB: If no error, len(fieldRes) == len(fields), and `toLoad` contains all metadata
	// for fields that need to be loaded from disk.
	fieldRes, toLoadMetas, err := s.processFields(fields)
	if err != nil {
		return nil, err
	}

	if len(toLoadMetas) == 0 {
		return fieldRes, nil
	}

	// NB: If no error, len(loadedFields) == len(toLoad).
	loaded, err := s.loadFields(toLoadMetas)
	if err != nil {
		for i := range fieldRes {
			if fieldRes[i] != nil {
				fieldRes[i].Close()
				fieldRes[i] = nil
			}
		}
		return nil, err
	}

	if err := s.insertFields(loaded, toLoadMetas); err != nil {
		for i := range fieldRes {
			if fieldRes[i] != nil {
				fieldRes[i].Close()
				fieldRes[i] = nil
			}
		}
		return nil, err
	}

	for i, meta := range toLoadMetas {
		fieldRes[meta.index] = loaded[i]
	}
	return fieldRes, nil
}

// NB(xichen): If needed, we could keep track of the fields that are currently
// being loaded (by other read requests) so that we don't load the same fields
// multiple times concurrently. This however only happens if there are simultaneous
// requests reading the same field at almost exactly the same time and therefore
// should be a relatively rare case so keeping the logic simple for now.
//
// Postcondition: If no error, `fieldRes` contains and owns the fields present in memory,
// and should be closed when processing is done. However, it is possible that some of
// the slots in `fieldRes` are nil if the corresponding fields don't exist in the segment.
// Otherwise if an error is returned, there is no need to close the field in `fieldRes` as
// that has been taken care of.
func (s *immutableSeg) processFields(fields []retrieveFieldOptions) (
	fieldRes []index.DocsField,
	toLoad []loadFieldMetadata,
	err error,
) {
	if len(fields) == 0 {
		return nil, nil, nil
	}

	fieldRes = make([]index.DocsField, len(fields))
	toLoad = make([]loadFieldMetadata, 0, len(fields))

	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, nil, errImmutableSegmentAlreadyClosed
	}

	for i, f := range fields {
		fieldHash := s.fieldHashFn(f.fieldPath)
		entry, exists := s.entries[fieldHash]
		if !exists {
			// If the field is not present in the field map, it means this field does not
			// belong to the segment and there is no need to attempt to load it from disk.
			continue
		}
		if entry.field != nil {
			// TODO(xichen): We always take the field as is if it is not null, without checking
			// if the field actually contains the required set of value types. For now
			// this is fine because when we retrieve values from disk, we always load the full
			// set of values regardless of value types requested. However this is not optimal
			// as we might end up loading more data than necessary. In the future we should optimize
			// the logic so that we only load the values for the field types that are requested.
			fieldRes[i] = entry.field.ShallowCopy()
			continue
		}
		// Otherwise we should load it from disk.
		loadMeta := loadFieldMetadata{
			retrieveOpts: f,
			index:        i,
			fieldHash:    fieldHash,
		}
		toLoad = append(toLoad, loadMeta)
	}

	return fieldRes, toLoad, nil
}

// NB(xichen): Fields are loaded sequentially, but can be parallelized using a worker
// pool when the need to do so arises.
func (s *immutableSeg) loadFields(metas []loadFieldMetadata) ([]index.DocsField, error) {
	res := make([]index.DocsField, 0, len(metas))
	for _, fm := range metas {
		// NB(xichen): This assumes that the loaded field is never nil if err == nil.
		loaded, err := s.retrieveFieldFromFSFn(fm.retrieveOpts)
		if err != nil {
			for i := range res {
				res[i].Close()
				res[i] = nil
			}
			return nil, err
		}
		res = append(res, loaded)
	}
	return res, nil
}

// Precondition: len(fields) == len(metas).
// Postcondition: If no error, `fields` contains and owns the fields loaded for `metas`,
// and should be closed when processing is done. Otherwise if an error is returned, there
// is no need to close the field in `fields` as that has been taken care of.
func (s *immutableSeg) insertFields(
	fields []index.DocsField,
	metas []loadFieldMetadata,
) error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		// Close all fields.
		for i := range fields {
			fields[i].Close()
			fields[i] = nil
		}
		return errImmutableSegmentAlreadyClosed
	}

	var multiErr xerrors.MultiError
	for i, meta := range metas {
		// Check the field map for other fields.
		entry, exists := s.entries[meta.fieldHash]
		if !exists {
			err := fmt.Errorf("field %v loaded but does not exist in segment field map", meta.retrieveOpts.fieldPath)
			multiErr = multiErr.Add(err)
			fields[i].Close()
			fields[i] = nil
			continue
		}
		if entry.field == nil {
			entry.field = fields[i].ShallowCopy()
			continue
		}
		fields[i].Close()
		fields[i] = entry.field.ShallowCopy()
	}

	s.status = segmentLoadedFromFS

	err := multiErr.FinalError()
	if err == nil {
		return nil
	}

	// Close all fields remaining.
	for i := range fields {
		if fields[i] != nil {
			fields[i].Close()
			fields[i] = nil
		}
	}

	return err
}

// applyFilters applies timestamp filters and other filters if applicable,
// and returns a doc ID iterator that outputs doc IDs matching the filtering criteria.
// nolint: unparam
// TODO(xichen): Collapse filters against the same field.
// TODO(xichen): Remove the nolint directive once the implementation is finished.
func applyFilters(
	startNanosInclusive, endNanosExclusive int64,
	filters []query.FilterList,
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []index.DocsField,
	numTotalDocs int32,
) (index.DocIDSetIterator, error) {
	timestampFieldIdx := fieldIndexMap[0]
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

	// Compute timestamp filter.
	docIDSet := timestampField.DocIDSet()
	timeIter, err := timestampFieldValues.Iter()
	if err != nil {
		return nil, err
	}
	docIDTimeIter := iterator.NewDocIDWithTimeIterator(docIDSet.Iter(), timeIter)
	timeRangeFilter := filter.NewTimeRangeFilter(startNanosInclusive, endNanosExclusive)
	filteredTimeIter := iterator.NewFilteredDocIDWithTimeIterator(docIDTimeIter, timeRangeFilter)
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

func applyFilter(
	flt query.Filter,
	fld index.DocsField,
	fieldTypes field.ValueTypeSet,
	numTotalDocs int32,
) (index.DocIDSetIterator, error) {
	// Determine whether the filter operator is a doc ID set filter.
	var docIDSetIteratorFn index.DocIDSetIteratorFn
	if flt.Op.IsDocIDSetFilter() {
		docIDSetIteratorFn = flt.Op.MustDocIDSetFilterFn(numTotalDocs)
	}

	if fld == nil || len(fieldTypes) == 0 {
		docIDIter := index.NewEmptyDocIDSetIterator()
		if docIDSetIteratorFn == nil {
			return docIDIter, nil
		}
		return docIDSetIteratorFn(docIDIter), nil
	}

	if len(fieldTypes) == 1 {
		var t field.ValueType
		for k := range fieldTypes {
			t = k
			break
		}
		return applyFilterForType(flt, fld, t, docIDSetIteratorFn)
	}

	combinator, err := flt.Op.MultiTypeCombinator()
	if err != nil {
		return nil, err
	}
	iters := make([]index.DocIDSetIterator, 0, len(fieldTypes))
	for t := range fieldTypes {
		iter, err := applyFilterForType(flt, fld, t, docIDSetIteratorFn)
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	}

	switch combinator {
	case filter.And:
		return index.NewInAllDocIDSetIterator(iters...), nil
	case filter.Or:
		return index.NewInAnyDocIDSetIterator(iters...), nil
	default:
		return nil, fmt.Errorf("unknown filter combinator %v", combinator)
	}
}

// applyFilterForType applies the filter against a given field with the given type,
// and returns a doc ID iterator. If no such field type exists, errFieldTypeNotFoundForFilter
// is returned.
func applyFilterForType(
	flt query.Filter,
	fld index.DocsField,
	ft field.ValueType,
	docIDSetIteratorFn index.DocIDSetIteratorFn,
) (index.DocIDSetIterator, error) {
	switch ft {
	case field.NullType:
		nullField, exists := fld.NullField()
		if !exists {
			return nil, errFieldTypeNotFoundForFilter
		}
		docIDIter := nullField.DocIDSet().Iter()
		if docIDSetIteratorFn != nil {
			return docIDSetIteratorFn(docIDIter), nil
		}
		return docIDIter, nil
	case field.BoolType:
		boolField, exists := fld.BoolField()
		if !exists {
			return nil, errFieldTypeNotFoundForFilter
		}
		docIDIter := boolField.DocIDSet().Iter()
		boolIter, err := boolField.Values().Iter()
		if err != nil {
			return nil, err
		}
		if docIDSetIteratorFn != nil {
			return docIDSetIteratorFn(docIDIter), nil
		}
		boolFilter, err := flt.BoolFilter()
		if err != nil {
			return nil, err
		}
		pairIter := iterator.NewDocIDWithBoolIterator(docIDIter, boolIter)
		return iterator.NewFilteredDocIDWithBoolIterator(pairIter, boolFilter), nil
	case field.IntType:
		intField, exists := fld.IntField()
		if !exists {
			return nil, errFieldTypeNotFoundForFilter
		}
		docIDIter := intField.DocIDSet().Iter()
		intIter, err := intField.Values().Iter()
		if err != nil {
			return nil, err
		}
		if docIDSetIteratorFn != nil {
			return docIDSetIteratorFn(docIDIter), nil
		}
		intFilter, err := flt.IntFilter()
		if err != nil {
			return nil, err
		}
		pairIter := iterator.NewDocIDWithIntIterator(docIDIter, intIter)
		return iterator.NewFilteredDocIDWithIntIterator(pairIter, intFilter), nil
	case field.DoubleType:
		doubleField, exists := fld.DoubleField()
		if !exists {
			return nil, errFieldTypeNotFoundForFilter
		}
		docIDIter := doubleField.DocIDSet().Iter()
		doubleIter, err := doubleField.Values().Iter()
		if err != nil {
			return nil, err
		}
		if docIDSetIteratorFn != nil {
			return docIDSetIteratorFn(docIDIter), nil
		}
		doubleFilter, err := flt.DoubleFilter()
		if err != nil {
			return nil, err
		}
		pairIter := iterator.NewDocIDWithDoubleIterator(docIDIter, doubleIter)
		return iterator.NewFilteredDocIDWithDoubleIterator(pairIter, doubleFilter), nil
	case field.StringType:
		stringField, exists := fld.StringField()
		if !exists {
			return nil, errFieldTypeNotFoundForFilter
		}
		docIDIter := stringField.DocIDSet().Iter()
		stringIter, err := stringField.Values().Iter()
		if err != nil {
			return nil, err
		}
		if docIDSetIteratorFn != nil {
			return docIDSetIteratorFn(docIDIter), nil
		}
		stringFilter, err := flt.StringFilter()
		if err != nil {
			return nil, err
		}
		pairIter := iterator.NewDocIDWithStringIterator(docIDIter, stringIter)
		return iterator.NewFilteredDocIDWithStringIterator(pairIter, stringFilter), nil
	case field.TimeType:
		timeField, exists := fld.TimeField()
		if !exists {
			return nil, errFieldTypeNotFoundForFilter
		}
		docIDIter := timeField.DocIDSet().Iter()
		timeIter, err := timeField.Values().Iter()
		if err != nil {
			return nil, err
		}
		if docIDSetIteratorFn != nil {
			return docIDSetIteratorFn(docIDIter), nil
		}
		timeFilter, err := flt.TimeFilter()
		if err != nil {
			return nil, err
		}
		pairIter := iterator.NewDocIDWithTimeIterator(docIDIter, timeIter)
		return iterator.NewFilteredDocIDWithTimeIterator(pairIter, timeFilter), nil
	default:
		return nil, fmt.Errorf("invalid field type %v for filtering", ft)
	}
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

type loadFieldMetadata struct {
	retrieveOpts retrieveFieldOptions
	index        int
	fieldHash    hash.Hash
}

type queryFieldMeta struct {
	fieldPath               []string
	allowedTypesBySourceIdx map[int]field.ValueTypeSet
}

// Precondition: m.fieldPath == other.fieldPath.
// Precondition: The set of source indices in the two metas don't overlap.
func (m *queryFieldMeta) MergeInPlace(other queryFieldMeta) {
	for idx, types := range other.allowedTypesBySourceIdx {
		m.allowedTypesBySourceIdx[idx] = types
	}
}
