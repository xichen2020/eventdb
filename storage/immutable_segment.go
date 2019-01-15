package storage

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/index"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"
	"github.com/xichen2020/eventdb/x/hash"

	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
)

// immutableSegment is an immutable segment.
type immutableSegment interface {
	immutableSegmentBase

	// QueryRaw returns results for a given raw query.
	QueryRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
	) ([]query.RawResult, error)

	// LoadedStatus returns the segment loaded status.
	LoadedStatus() segmentLoadedStatus

	// Unload unloads all fields from memory.
	Unload() error

	// Flush flushes the segment to persistent storage.
	Flush(persistFns persist.Fns) error
}

const (
	defaultRawResultsCapacity = 4096
)

var (
	errImmutableSegmentAlreadyClosed         = errors.New("immutable segment is already closed")
	errFlushingNotInMemoryOnlySegment        = errors.New("flushing a segment that is not in memory only")
	errDataNotAvailableInInMemoryOnlySegment = errors.New("data unavaible for in-memory only segment")
	errNoTimeValuesInTimestampField          = errors.New("no time values in timestamp field")
	errNoStringValuesInRawDocSourceField     = errors.New("no string values in raw doc source field")
)

type segmentLoadedStatus int

const (
	// The full segment is loaded in memory.
	segmentFullyLoaded segmentLoadedStatus = iota

	// The segment is partially loaded in memory.
	segmentPartiallyLoaded

	// The full segment has been unloaded onto disk.
	segmentUnloaded
)

type segmentDataLocation int

const (
	unknownLocation segmentDataLocation = iota

	// The segment data is only available in memory.
	inMemoryOnly

	// The segment data is available on disk and may or may not be in memory.
	availableOnDisk
)

type immutableSegmentOptions struct {
	timestampFieldPath []string
	rawDocSourcePath   []string
	fieldHashFn        hash.StringArrayHashFn
	fieldRetriever     persist.FieldRetriever
}

type immutableSeg struct {
	sync.RWMutex
	immutableSegmentBase

	namespace          []byte
	shard              uint32
	timestampFieldPath []string
	rawDocSourcePath   []string
	fieldHashFn        hash.StringArrayHashFn
	segmentMeta        persist.SegmentMetadata
	fieldRetriever     persist.FieldRetriever

	closed       bool
	loadedStatus segmentLoadedStatus
	dataLocation segmentDataLocation
	entries      map[hash.Hash]*fieldEntry
}

type fieldEntry struct {
	fieldMeta indexfield.DocsFieldMetadata
	field     indexfield.DocsField
}

// nolint: unparam
func newImmutableSegment(
	namespace []byte,
	shard uint32,
	id string,
	numDocs int32,
	minTimeNanos, maxTimeNanos int64,
	loadedStatus segmentLoadedStatus,
	dataLocation segmentDataLocation,
	fields map[hash.Hash]indexfield.DocsField,
	opts immutableSegmentOptions,
) *immutableSeg {
	segmentMeta := persist.SegmentMetadata{
		ID:           id,
		MinTimeNanos: minTimeNanos,
		MaxTimeNanos: maxTimeNanos,
	}
	entries := make(map[hash.Hash]*fieldEntry, len(fields))
	for k, f := range fields {
		entries[k] = &fieldEntry{
			fieldMeta: f.Metadata(),
			field:     f,
		}
	}
	return &immutableSeg{
		immutableSegmentBase: newBaseSegment(id, numDocs, minTimeNanos, maxTimeNanos),
		namespace:            namespace,
		shard:                shard,
		timestampFieldPath:   opts.timestampFieldPath,
		rawDocSourcePath:     opts.rawDocSourcePath,
		fieldHashFn:          opts.fieldHashFn,
		segmentMeta:          segmentMeta,
		fieldRetriever:       opts.fieldRetriever,
		loadedStatus:         loadedStatus,
		dataLocation:         dataLocation,
		entries:              entries,
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
	q query.ParsedRawQuery,
) ([]query.RawResult, error) {
	// Fast path if the limit indicates no results are needed.
	if q.Limit != nil && *q.Limit <= 0 {
		return nil, nil
	}

	// Identify the set of fields needed for query execution.
	allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, err := s.collectFieldsForRawQuery(q)
	if err != nil {
		return nil, err
	}

	// Validate that the fields to order results by have one and only one field type.
	hasEmptyResult, err := validateOrderByClauses(allowedFieldTypes, q.OrderBy)
	if err != nil {
		return nil, err
	}
	if hasEmptyResult {
		return nil, nil
	}

	// Retrieve all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Raw doc source field
	// - Fields in `filters` if applicable
	// - Fields in `orderBy` if applicable
	queryFields, err := s.retrieveFields(fieldsToRetrieve)
	if err != nil {
		return nil, err
	}
	defer func() {
		for i := range queryFields {
			if queryFields[i] != nil {
				queryFields[i].Close()
				queryFields[i] = nil
			}
		}
	}()

	rawDocSourceField, ok := queryFields[1].StringField()
	if !ok {
		return nil, errNoStringValuesInRawDocSourceField
	}

	// Apply filters to determine the doc ID set matching the filters.
	filteredDocIDIter, err := applyFilters(
		q.StartNanosInclusive, q.EndNanosExclusive, q.Filters,
		allowedFieldTypes, fieldIndexMap, queryFields, s.NumDocuments(),
	)
	if err != nil {
		return nil, err
	}

	if len(q.OrderBy) == 0 {
		return collectUnorderedRawDocSourceData(rawDocSourceField, filteredDocIDIter, q.Limit)
	}

	return collectOrderedRawDocSourceData(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		rawDocSourceField,
		filteredDocIDIter,
		q.OrderBy,
		q.RawResultLessThanFn,
		q.Limit,
	)
}

func (s *immutableSeg) LoadedStatus() segmentLoadedStatus {
	s.RLock()
	loadedStatus := s.loadedStatus
	s.RUnlock()
	return loadedStatus
}

func (s *immutableSeg) Unload() error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errImmutableSegmentAlreadyClosed
	}
	if s.loadedStatus == segmentUnloaded {
		return nil
	}
	s.loadedStatus = segmentUnloaded
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

	if !(s.loadedStatus == segmentFullyLoaded && s.dataLocation == inMemoryOnly) {
		// NB: This should never happen.
		s.RUnlock()
		return errFlushingNotInMemoryOnlySegment
	}

	// flushing non in-memory-only segmentcase so it's okay to allocate
	// here for better readability than caching the buffer as a field.
	fieldBuf := make([]indexfield.DocsField, 0, len(s.entries))
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

	if err == nil {
		s.Lock()
		s.dataLocation = availableOnDisk
		s.Unlock()
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
	q query.ParsedRawQuery,
) (
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	fieldsToRetrieve []persist.RetrieveFieldOptions,
	err error,
) {
	// Intersect the allowed field types determined from the given query
	// with the available field types in the segment.
	fieldMap := s.intersectWithAvailableTypes(q.AllowedFieldTypes)

	// Flatten the list of fields.
	numFieldsForQuery := q.NumFieldsForQuery()
	allowedFieldTypes = make([]field.ValueTypeSet, numFieldsForQuery)
	fieldIndexMap = make([]int, numFieldsForQuery)
	fieldsToRetrieve = make([]persist.RetrieveFieldOptions, 0, len(fieldMap))
	fieldIndex := 0
	for _, f := range fieldMap {
		allAllowedTypes := make(field.ValueTypeSet)
		for sourceIdx, types := range f.AllowedTypesBySourceIdx {
			allowedFieldTypes[sourceIdx] = types
			fieldIndexMap[sourceIdx] = fieldIndex
			for t := range types {
				allAllowedTypes[t] = struct{}{}
			}
		}
		fieldOpts := persist.RetrieveFieldOptions{
			FieldPath:  f.FieldPath,
			FieldTypes: allAllowedTypes,
		}
		fieldsToRetrieve = append(fieldsToRetrieve, fieldOpts)
		fieldIndex++
	}
	return allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, nil
}

// intersectWithAvailableTypes intersects the allowed field types derived from
// the parsed raw query with the set of available field types in the segment,
// returning mappings from the field hashes to the set of allowed and available field types.
//
// NB(xichen): The field path and types in the `entries` map don't change except
// when being closed. If we are here, it means there is an active query in which
// case `Close` will block until the query is done, and as a result there is no
// need to RLock here.
func (s *immutableSeg) intersectWithAvailableTypes(
	fm map[hash.Hash]query.FieldMeta,
) map[hash.Hash]query.FieldMeta {
	clonedFieldMap := make(map[hash.Hash]query.FieldMeta, len(fm))
	for k, meta := range fm {
		clonedMeta := query.FieldMeta{
			FieldPath:               meta.FieldPath,
			AllowedTypesBySourceIdx: make(map[int]field.ValueTypeSet, len(meta.AllowedTypesBySourceIdx)),
		}
		var availableTypes []field.ValueType
		entry, exists := s.entries[k]
		if exists {
			availableTypes = entry.fieldMeta.FieldTypes
		}
		for srcIdx, allowedTypes := range meta.AllowedTypesBySourceIdx {
			intersectedTypes := intersectFieldTypes(availableTypes, allowedTypes)
			clonedMeta.AllowedTypesBySourceIdx[srcIdx] = intersectedTypes
		}
		clonedFieldMap[k] = clonedMeta
	}
	return clonedFieldMap
}

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

// retrieveFields returns the set of field for a list of field retrieving options.
// If no error is returned, the result array contains the same number of slots
// as the number of fields to retrieve. Fields that don't exist in the segment
// will have a nil slot.
func (s *immutableSeg) retrieveFields(
	fields []persist.RetrieveFieldOptions,
) ([]indexfield.DocsField, error) {
	// Gather fields that are already loaded and identify fields that need to be retrieved
	// from filesystem.
	// NB: If no error, len(fieldRes) == len(fields), and `toLoad` contains all metadata
	// for fields that need to be loaded from disk.
	fieldRes, toLoadMetas, dataLocation, err := s.processFields(fields)
	if err != nil {
		return nil, err
	}

	if len(toLoadMetas) == 0 {
		return fieldRes, nil
	}

	cleanup := func() {
		for i := range fieldRes {
			if fieldRes[i] != nil {
				fieldRes[i].Close()
				fieldRes[i] = nil
			}
		}
	}

	if dataLocation == inMemoryOnly {
		// We have fields that are in the in-memory metadata hash, and the actual data
		// is not in memory, and yet the location indicates all data are in memory. This
		// is a logical error and should never happen.
		cleanup()
		return nil, errDataNotAvailableInInMemoryOnlySegment
	}

	// NB: If no error, len(loadedFields) == len(toLoad).
	loaded, err := s.loadFields(toLoadMetas)
	if err != nil {
		cleanup()
		return nil, err
	}

	if err := s.insertFields(loaded, toLoadMetas); err != nil {
		cleanup()
		return nil, err
	}

	for i, meta := range toLoadMetas {
		if fieldRes[meta.index] == nil {
			// All types are retrieved from filesystem.
			fieldRes[meta.index] = loaded[i]
		} else {
			// Some types are retrieved from memory, and others are retrieved from filesystem
			// so they need to be merged.
			fieldRes[meta.index].MergeInPlace(loaded[i])
			loaded[i].Close()
			loaded[i] = nil
		}
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
func (s *immutableSeg) processFields(fields []persist.RetrieveFieldOptions) (
	fieldRes []indexfield.DocsField,
	toLoad []loadFieldMetadata,
	dataLocation segmentDataLocation,
	err error,
) {
	if len(fields) == 0 {
		return nil, nil, unknownLocation, nil
	}

	fieldRes = make([]indexfield.DocsField, len(fields))
	toLoad = make([]loadFieldMetadata, 0, len(fields))

	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, nil, unknownLocation, errImmutableSegmentAlreadyClosed
	}

	for i, f := range fields {
		fieldHash := s.fieldHashFn(f.FieldPath)
		entry, exists := s.entries[fieldHash]
		if !exists {
			// If the field is not present in the field map, it means this field does not
			// belong to the segment and there is no need to attempt to load it from disk.
			continue
		}
		if entry.field != nil {
			// Determine if the field has all the types needed to be retrieved. The types
			// to retrieve are guaranteed to be a subset of field types available.
			retrieved, remainder, err := entry.field.NewDocsFieldFor(f.FieldTypes)
			if err != nil {
				// Close all the fields gathered so far.
				for idx := 0; idx < i; idx++ {
					if fieldRes[idx] != nil {
						fieldRes[idx].Close()
						fieldRes[idx] = nil
					}
				}
				return nil, nil, unknownLocation, err
			}
			fieldRes[i] = retrieved
			if len(remainder) == 0 {
				// All types to retrieve have been retrieved.
				continue
			}

			// Still more types to retrieve.
			retrieveOpts := persist.RetrieveFieldOptions{
				FieldPath:  f.FieldPath,
				FieldTypes: remainder,
			}
			loadMeta := loadFieldMetadata{
				retrieveOpts: retrieveOpts,
				index:        i,
				fieldHash:    fieldHash,
			}
			toLoad = append(toLoad, loadMeta)
			continue
		}

		// Otherwise we should load all types from disk.
		loadMeta := loadFieldMetadata{
			retrieveOpts: f,
			index:        i,
			fieldHash:    fieldHash,
		}
		toLoad = append(toLoad, loadMeta)
	}

	return fieldRes, toLoad, s.dataLocation, nil
}

// NB(xichen): Fields are loaded sequentially, but can be parallelized using a worker
// pool when the need to do so arises.
func (s *immutableSeg) loadFields(metas []loadFieldMetadata) ([]indexfield.DocsField, error) {
	if len(metas) == 0 {
		return nil, nil
	}
	res := make([]indexfield.DocsField, 0, len(metas))
	for _, fm := range metas {
		// NB(xichen): This assumes that the loaded field is never nil if err == nil.
		loaded, err := s.fieldRetriever.RetrieveField(
			s.namespace,
			s.shard,
			s.segmentMeta,
			fm.retrieveOpts,
		)
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
	fields []indexfield.DocsField,
	metas []loadFieldMetadata,
) error {
	if len(fields) == 0 {
		return nil
	}

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
			err := fmt.Errorf("field %v loaded but does not exist in segment field map", meta.retrieveOpts.FieldPath)
			multiErr = multiErr.Add(err)
			fields[i].Close()
			fields[i] = nil
			continue
		}
		if entry.field == nil {
			entry.field = fields[i].ShallowCopy()
			continue
		}
		// Merge what's been loaded into the existing field, but leave the loaded field unchanged.
		entry.field.MergeInPlace(fields[i])
	}

	s.loadedStatus = segmentPartiallyLoaded

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
// TODO(xichen): Check for iteration error at the end.
func applyFilters(
	startNanosInclusive, endNanosExclusive int64,
	filters []query.FilterList,
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
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
	limit *int,
) ([]query.RawResult, error) {
	// Return unordered results
	rawDocSourceIter, err := rawDocSourceField.Fetch(maskingDocIDSetIt)
	if err != nil {
		maskingDocIDSetIt.Close()
		return nil, fmt.Errorf("error fetching raw doc source data: %v", err)
	}
	defer rawDocSourceIter.Close()

	resultsCapacity := defaultRawResultsCapacity
	if limit != nil && *limit >= 0 {
		resultsCapacity = *limit
	}
	// TODO(xichen): Pool the results array.
	rawResults := make([]query.RawResult, 0, resultsCapacity)
	for rawDocSourceIter.Next() {
		rawResults = append(rawResults, query.RawResult{Data: rawDocSourceIter.Value()})
		if limit != nil && len(rawResults) >= *limit {
			break
		}
	}
	if err := rawDocSourceIter.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over raw doc source data: %v", err)
	}
	return rawResults, nil
}

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectOrderedRawDocSourceData(
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	rawDocSourceField indexfield.StringField,
	maskingDocIDSetIter index.DocIDSetIterator,
	orderBy []query.OrderBy,
	lessThanFn query.RawResultLessThanFn,
	limit *int,
) ([]query.RawResult, error) {
	filteredOrderByIter, err := createFilteredOrderByIterator(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		maskingDocIDSetIter,
		orderBy,
	)
	if err != nil {
		// TODO(xichen): Add filteredDocIDIter.Err() here.
		maskingDocIDSetIter.Close()
		maskingDocIDSetIter = nil
		return nil, err
	}
	defer filteredOrderByIter.Close()

	orderedRawResults, err := collectTopNRawResultDocIDOrderByValues(
		filteredOrderByIter,
		lessThanFn,
		limit,
	)
	if err != nil {
		return nil, err
	}

	return collectTopNRawResults(rawDocSourceField, orderedRawResults)
}

// NB: If an error is encountered, `maskingDocIDSetIter` should be closed at the callsite.
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

	orderByMultiIter := indexfield.NewMultiFieldIterator(orderByIters)
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
	limit *int,
) ([]query.RawResult, error) {
	// TODO(xichen): This algorithm runs in O(Nlogk) time. Should investigate whethere this is
	// in practice faster than first selecting top N values via a selection sort algorithm that
	// runs in O(N) time, then sorting the results in O(klogk) time.

	// Inserting values into the heap to select the top results based on the query ordering.
	heapCapacity := -1
	if limit != nil {
		heapCapacity = *limit
	}
	results := query.NewRawResultHeap(heapCapacity, lessThanFn)
	for filteredOrderByIter.Next() {
		docID := filteredOrderByIter.DocID()
		values := filteredOrderByIter.Values() // values here is only valid till the next iteration.
		dv := query.RawResult{DocID: docID, OrderByValues: values}
		if heapCapacity == -1 || results.Len() <= heapCapacity {
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
) ([]query.RawResult, error) {
	for i := 0; i < len(orderedRawResults); i++ {
		orderedRawResults[i].OrderIdx = i
	}
	sort.Sort(query.RawResultsByDocIDAsc(orderedRawResults))
	docIDValuesIt := query.NewRawResultIterator(orderedRawResults)
	rawDocSourceIter, err := rawDocSourceField.Fetch(docIDValuesIt)
	if err != nil {
		return nil, fmt.Errorf("error fetching raw doc source data: %v", err)
	}
	numItemsWithRawDocSource := 0
	for rawDocSourceIter.Next() {
		maskingPos := rawDocSourceIter.MaskingPosition()
		orderedRawResults[maskingPos].HasData = true
		orderedRawResults[maskingPos].Data = rawDocSourceIter.Value()
		numItemsWithRawDocSource++
	}
	if err := rawDocSourceIter.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over raw doc source field: %v", err)
	}
	sort.Sort(query.RawResultsByOrderIdxAsc(orderedRawResults))
	orderedRawResults = orderedRawResults[:numItemsWithRawDocSource]
	return orderedRawResults, nil
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
	retrieveOpts persist.RetrieveFieldOptions
	index        int
	fieldHash    hash.Hash
}
