package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/hash"

	xerrors "github.com/m3db/m3x/errors"
)

// TODO(xichen): Reuse field values or cache decoded values from filter iterators to avoid
// decoding the same field multiple times.

// immutableSegment is an immutable segment.
type immutableSegment interface {
	immutableSegmentBase

	// QueryRaw queries results for a given raw query.
	// Existing results if any (e.g., from querying other segments) are passed in `res`
	// to help facilitate fast elimination of segments that do not have eligible records
	// for the given query. The results from the current segment if any are merged with
	// those in `res` upon completion.
	QueryRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
		res *query.RawResults,
	) error

	// QueryGrouped queries results for a given grouped query. The results from the current
	// segment if any are merged with those in `res` upon completion.
	QueryGrouped(
		ctx context.Context,
		q query.ParsedGroupedQuery,
		res *query.GroupedResults,
	) error

	// QueryTimeBucket queries results for a given time bucket query. The results from the
	// current segment if any are merged with those in `res` upon completion.
	QueryTimeBucket(
		ctx context.Context,
		q query.ParsedTimeBucketQuery,
		res *query.TimeBucketResults,
	) error

	// LoadedStatus returns the segment loaded status.
	LoadedStatus() segmentLoadedStatus

	// Unload unloads all fields from memory.
	Unload() error

	// Flush flushes the segment to persistent storage.
	Flush(persistFns persist.Fns) error
}

var (
	errImmutableSegmentAlreadyClosed         = errors.New("immutable segment is already closed")
	errFlushingNotInMemoryOnlySegment        = errors.New("flushing a segment that is not in memory only")
	errDataNotAvailableInInMemoryOnlySegment = errors.New("data unavailable for in-memory only segment")
	errNoTimeValuesInTimestampField          = errors.New("no time values in timestamp field")
	errNoTimestampField                      = errors.New("no timestamp field in the segment")
	errNoRawDocSourceField                   = errors.New("no raw doc source field in the segment")
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
	fieldPath  []string
	fieldTypes []field.ValueType
	valuesMeta []values.MetaUnion
	field      indexfield.DocsField
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
		fm := f.Metadata()
		valuesMeta := make([]values.MetaUnion, 0, len(fm.FieldTypes))
		for _, t := range fm.FieldTypes {
			tf, _ := f.FieldForType(t)
			valuesMeta = append(valuesMeta, tf.MustValuesMeta())
		}
		entries[k] = &fieldEntry{
			fieldPath:  fm.FieldPath,
			fieldTypes: fm.FieldTypes,
			valuesMeta: valuesMeta,
			field:      f,
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
	res *query.RawResults,
) error {
	// Fast path if the limit indicates no results are needed.
	if res.IsComplete() {
		return nil
	}

	if s.NumDocuments() == 0 {
		return nil
	}

	// Check if this segment has all orderBy fields as they are required, and bail early
	// otherwise.
	for _, ob := range q.OrderBy {
		fieldHash := s.fieldHashFn(ob.FieldPath)
		if _, exists := s.entries[fieldHash]; !exists {
			return nil
		}
	}

	if res.HasOrderedFilter() && res.Len() > 0 && res.LimitReached() {
		// We only proceed to check if the orderBy values are in range if we can terminate
		// early. As such, the result needs to support ordered filtering and the result
		// set size should have reached the query limit.
		mayBeInRange, err := s.orderByValuesMaybeInRange(q.OrderBy, res)
		if err != nil {
			return err
		}
		if !mayBeInRange {
			return nil
		}
	}

	// Identify the set of fields needed for query execution.
	allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, err := s.collectFieldsForQuery(
		q.NumFieldsForQuery(),
		q.FieldConstraints,
	)
	if err != nil {
		return err
	}

	// Retrieve all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Raw doc source field
	// - Fields in `filters` if applicable
	// - Fields in `orderBy` if applicable
	queryFields, err := s.retrieveFields(fieldsToRetrieve)
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
		q.TimestampFieldIndex(), q.FilterStartIndex(), fieldIndexMap, queryFields, s.NumDocuments(),
	)
	if err != nil {
		return err
	}

	queryRawDocSourceFieldIdx := fieldIndexMap[q.RawDocSourceFieldIndex()]
	if queryFields[queryRawDocSourceFieldIdx] == nil {
		return errNoRawDocSourceField
	}
	rawDocSourceField, ok := queryFields[queryRawDocSourceFieldIdx].StringField()
	if !ok {
		return errNoStringValuesInRawDocSourceField
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

func (s *immutableSeg) QueryGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
	res *query.GroupedResults,
) error {
	// Fast path if the limit indicates no results are needed.
	if res.IsComplete() {
		return nil
	}

	if s.NumDocuments() == 0 {
		return nil
	}

	// Check if this segment has all groupBy fields as they are required, and bail early
	// otherwise.
	for _, gb := range q.GroupBy {
		fieldHash := s.fieldHashFn(gb)
		if _, exists := s.entries[fieldHash]; !exists {
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
		fieldHash := s.fieldHashFn(calc.FieldPath)
		if _, exists := s.entries[fieldHash]; exists {
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
	allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, err := s.collectFieldsForQuery(
		q.NumFieldsForQuery(),
		q.FieldConstraints,
	)
	if err != nil {
		return err
	}

	// Retrieve all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Fields in `filters` if applicable
	// - Fields in `groupBy` if applicable
	// - Fields in `calculation` if applicable
	// Fields in `orderBy` should appear either in `groupBy` or `calculation` clauses
	// and thus are also covered.
	queryFields, err := s.retrieveFields(fieldsToRetrieve)
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
		q.TimestampFieldIndex(), q.FilterStartIndex(), fieldIndexMap, queryFields, s.NumDocuments(),
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

func (s *immutableSeg) QueryTimeBucket(
	ctx context.Context,
	q query.ParsedTimeBucketQuery,
	res *query.TimeBucketResults,
) error {
	if s.NumDocuments() == 0 {
		return nil
	}

	// Identify the set of fields needed for query execution.
	allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, err := s.collectFieldsForQuery(
		q.NumFieldsForQuery(),
		q.FieldConstraints,
	)
	if err != nil {
		return err
	}

	// Retrieve all fields (possibly from disk) identified above.
	// NB: The items in the field index map are in the following order:
	// - Timestamp field
	// - Fields in `filters` if applicable
	queryFields, err := s.retrieveFields(fieldsToRetrieve)
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
		q.TimestampFieldIndex(), q.FilterStartIndex(), fieldIndexMap, queryFields, s.NumDocuments(),
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
	// Nil out the field values but keep the metadata so the segment
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

// orderByValuesMaybeInRange checks if the orderBy field values in this segment fall
// within the range of orderBy field values of the existing results.
// Precondition: The orderBy fields are guaranteed to exist, and the result set is guaranteed
// to support ordered filter and has reached the query limit.
func (s *immutableSeg) orderByValuesMaybeInRange(
	orderBy []query.OrderBy,
	res *query.RawResults,
) (bool, error) {
	var (
		minOrderByValuesInResults = res.MinOrderByValues()
		maxOrderByValuesInResults = res.MaxOrderByValues()
		minOrderByValuesInSegment []field.ValueUnion
		maxOrderByValuesInSegment []field.ValueUnion
	)
	for i, ob := range orderBy {
		orderByFieldHash := s.fieldHashFn(ob.FieldPath)
		entry, exists := s.entries[orderByFieldHash]
		if !exists {
			// This segment does not have one of the field to order results by,
			// as such we bail early as we require the orderBy field be present in
			// the result documents. This should never happen as part of the precondition
			// but just to be extra careful.
			return false, nil
		}
		availableTypes := entry.fieldTypes
		if len(availableTypes) > 1 {
			// We do not allow the orderBy field to have more than one type.
			return false, fmt.Errorf("order by field %v has multiple types %v", ob.FieldPath, availableTypes)
		}
		if minOrderByValuesInResults[i].Type != availableTypes[0] {
			// We expect the orderBy fields to have consistent types.
			return false, fmt.Errorf("order by field have type %v in the results and type %v in the segment", minOrderByValuesInResults[i].Type, availableTypes[0])
		}
		valuesMeta := entry.valuesMeta[0]
		minUnion, maxUnion, err := valuesMeta.ToMinMaxValueUnion()
		if err != nil {
			return false, err
		}
		if minOrderByValuesInSegment == nil {
			minOrderByValuesInSegment = make([]field.ValueUnion, 0, len(orderBy))
			maxOrderByValuesInSegment = make([]field.ValueUnion, 0, len(orderBy))
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

func (s *immutableSeg) collectFieldsForQuery(
	numFieldsForQuery int,
	fieldConstraints map[hash.Hash]query.FieldMeta,
) (
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	fieldsToRetrieve []persist.RetrieveFieldOptions,
	err error,
) {
	// Intersect the allowed field types determined from the given query
	// with the available field types in the segment.
	fieldMap := s.intersectWithAvailableTypes(fieldConstraints)

	// Flatten the list of fields.
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
	fieldConstraints map[hash.Hash]query.FieldMeta,
) map[hash.Hash]query.FieldMeta {
	clonedFieldMap := make(map[hash.Hash]query.FieldMeta, len(fieldConstraints))
	for k, meta := range fieldConstraints {
		clonedMeta := query.FieldMeta{
			FieldPath:               meta.FieldPath,
			AllowedTypesBySourceIdx: make(map[int]field.ValueTypeSet, len(meta.AllowedTypesBySourceIdx)),
		}
		var availableTypes []field.ValueType
		entry, exists := s.entries[k]
		if exists {
			availableTypes = entry.fieldTypes
		}
		for srcIdx, allowedTypes := range meta.AllowedTypesBySourceIdx {
			intersectedTypes := intersectFieldTypes(availableTypes, allowedTypes)
			clonedMeta.AllowedTypesBySourceIdx[srcIdx] = intersectedTypes
		}
		clonedFieldMap[k] = clonedMeta
	}
	return clonedFieldMap
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

type loadFieldMetadata struct {
	retrieveOpts persist.RetrieveFieldOptions
	index        int
	fieldHash    hash.Hash
}
