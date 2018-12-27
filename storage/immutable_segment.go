package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/x/hash"

	"github.com/m3db/m3x/context"
)

type retrieveFieldOptions struct {
	fieldPath []string

	// If this is true, all available field values are retrieved regardless
	// of the field type. The `fieldTypes` field is ignored.
	allFieldTypes bool

	// If allFieldTypes is false, only field values associated with the field
	// types in the `fieldTypes` map are retrieved.
	fieldTypes map[field.ValueType]struct{}
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
)

type segmentLoadedStatus int

const (
	segmentLoadedFullyInMem segmentLoadedStatus = iota
	segmentLoadedFromFS
	segmentUnloaded
)

var (
	errNoEligibleFieldTypesForQuery = errors.New("no eligible field types for query")
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

	closed bool
	status segmentLoadedStatus
	// TODO(xichen): Potentially keep track of the set of value types seen per field.
	fields map[hash.Hash]document.DocsField
}

// nolint: unparam
func newImmutableSegment(
	id string,
	numDocs int32,
	minTimeNanos, maxTimeNanos int64,
	status segmentLoadedStatus,
	fields map[hash.Hash]document.DocsField,
	opts immutableSegmentOptions,
) *immutableSeg {
	return &immutableSeg{
		immutableSegmentBase:  newBaseSegment(id, numDocs, minTimeNanos, maxTimeNanos),
		timestampFieldPath:    opts.timestampFieldPath,
		rawDocSourcePath:      opts.rawDocSourcePath,
		fieldHashFn:           opts.fieldHashFn,
		retrieveFieldFromFSFn: opts.retrieveFieldFromFSFn,
		status:                status,
		fields:                fields,
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
	timestampFieldIdx, rawDocSourceFieldIdx, fieldsToRetrieve, err := s.collectFieldsForRawQuery(filters, orderBy)
	if err == errNoEligibleFieldTypesForQuery {
		return query.RawResult{}, nil
	}
	if err != nil && err != errNoEligibleFieldTypesForQuery {
		return query.RawResult{}, err
	}

	// Retrieve all fields (possibly from disk) identified above.
	queryFields, err := s.retrieveFields(fieldsToRetrieve)
	if err != nil {
		return query.RawResult{}, err
	}
	timestampField := queryFields[timestampFieldIdx]
	rawDocSourceField := queryFields[rawDocSourceFieldIdx]

	// TODO(xichen): Apply timestamp filter and other filters if applicable.
	_, _, _ = timestampField, rawDocSourceField, queryFields
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
	// Nil out the field values but keep the field keys so the segment
	// can easily determine whether a field needs to be loaded from disk.
	for k, f := range s.fields {
		f.Close()
		s.fields[k] = nil
	}
	return nil
}

func (s *immutableSeg) Flush(persistFns persist.Fns) error {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return errImmutableSegmentAlreadyClosed
	}

	// NB: Segment is only flushed once as a common case so it's okay to allocate
	// here for better readability than caching the buffer as a field.
	fieldBuf := make([]document.DocsField, 0, len(s.fields))
	for _, f := range s.fields {
		fieldBuf = append(fieldBuf, f)
	}

	return persistFns.WriteFields(fieldBuf)
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
	for _, f := range s.fields {
		f.Close()
	}
	s.fields = nil
}

func (s *immutableSeg) collectFieldsForRawQuery(
	filters []query.FilterList,
	orderBy []query.OrderBy,
) (
	timestampFieldIdx int,
	rawDocSourceFieldIdx int,
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
	isTimestampField := true
	meta := queryFieldMeta{
		fieldPath:        s.timestampFieldPath,
		isTimestampField: &isTimestampField,
		allowedFieldTypes: map[field.ValueType]struct{}{
			field.TimeType: struct{}{},
		},
	}
	if err := s.addQueryFieldToMap(fieldMap, meta); err != nil {
		return 0, 0, nil, err
	}

	// Insert raw doc source field.
	isRawDocSourceField := true
	meta = queryFieldMeta{
		fieldPath:           s.rawDocSourcePath,
		isRawDocSourceField: &isRawDocSourceField,
		allowedFieldTypes: map[field.ValueType]struct{}{
			field.StringType: struct{}{},
		},
	}
	if err := s.addQueryFieldToMap(fieldMap, meta); err != nil {
		return 0, 0, nil, err
	}

	// Insert filter fields.
	for _, fl := range filters {
		for _, f := range fl.Filters {
			meta := queryFieldMeta{
				fieldPath: f.FieldPath,
			}
			if f.Value == nil {
				meta.allowAllFieldTypes = true
			} else {
				comparableTypes, err := f.Value.Type.ComparableTypes()
				if err != nil {
					return 0, 0, nil, err
				}
				meta.allowedFieldTypes = toFieldTypeMap(comparableTypes)
			}
			if err := s.addQueryFieldToMap(fieldMap, meta); err != nil {
				return 0, 0, nil, err
			}
		}
	}

	// Insert order by fields.
	for _, ob := range orderBy {
		meta := queryFieldMeta{
			fieldPath:          ob.FieldPath,
			allowAllFieldTypes: true,
		}
		if err := s.addQueryFieldToMap(fieldMap, meta); err != nil {
			return 0, 0, nil, err
		}
	}

	// Flatten the list of fields.
	timestampFieldIdx = -1
	rawDocSourceFieldIdx = -1
	fieldsToRetrieve = make([]retrieveFieldOptions, 0, len(fieldMap))
	idx := 0
	for _, f := range fieldMap {
		if f.isTimestampField != nil && *f.isTimestampField {
			timestampFieldIdx = idx
		}
		if f.isRawDocSourceField != nil && *f.isRawDocSourceField {
			rawDocSourceFieldIdx = idx
		}
		fieldOpts := retrieveFieldOptions{
			fieldPath:     f.fieldPath,
			allFieldTypes: f.allowAllFieldTypes,
			fieldTypes:    f.allowedFieldTypes,
		}
		fieldsToRetrieve = append(fieldsToRetrieve, fieldOpts)
		idx++
	}
	return timestampFieldIdx, rawDocSourceFieldIdx, fieldsToRetrieve, nil
}

func toFieldTypeMap(fieldTypes []field.ValueType) map[field.ValueType]struct{} {
	m := make(map[field.ValueType]struct{}, len(fieldTypes))
	for _, ft := range fieldTypes {
		m[ft] = struct{}{}
	}
	return m
}

// retrieveFields returns the set of field for a list of field retrieving options.
// If no error is returned, the result array contains the same number of slots
// as the number of fields to retrieve. Fields that don't exist in the segment
// will have a nil slot.
func (s *immutableSeg) retrieveFields(
	fields []retrieveFieldOptions,
) ([]document.ReadOnlyDocsField, error) {
	if len(fields) == 0 {
		return nil, nil
	}

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
		return nil, err
	}

	if err := s.insertFields(loaded, toLoadMetas); err != nil {
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
func (s *immutableSeg) processFields(fields []retrieveFieldOptions) (
	fieldRes []document.ReadOnlyDocsField,
	toLoad []loadFieldMetadata,
	err error,
) {
	fieldRes = make([]document.ReadOnlyDocsField, len(fields))
	toLoad = make([]loadFieldMetadata, 0, len(fields))

	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, nil, errImmutableSegmentAlreadyClosed
	}

	for i, f := range fields {
		fieldHash := s.fieldHashFn(f.fieldPath)
		b, exists := s.fields[fieldHash]
		if !exists {
			// If the field is not present in the field map, it means this field does not
			// belong to the segment and there is no need to attempt to load it from disk.
			continue
		}
		if b != nil {
			// TODO(xichen): We always take the field as is if it is not null, without checking
			// if the field actually contains the required set of value types. For now
			// this is fine because when we retrieve values from disk, we always load the full
			// set of values regardless of value types requested. However this is not optimal
			// as we might end up loading more data than necessary. In the future we should optimize
			// the logic so that we only load the values for the field types that are requested.
			fieldRes[i] = b
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
func (s *immutableSeg) loadFields(metas []loadFieldMetadata) ([]document.DocsField, error) {
	res := make([]document.DocsField, 0, len(metas))
	for _, fm := range metas {
		loaded, err := s.retrieveFieldFromFSFn(fm.retrieveOpts)
		if err != nil {
			return nil, err
		}
		res = append(res, loaded)
	}
	return res, nil
}

// Precondition: len(fields) == len(metas).
func (s *immutableSeg) insertFields(
	fields []document.DocsField,
	metas []loadFieldMetadata,
) error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		// Close all fields.
		for _, f := range fields {
			f.Close()
		}
		return errImmutableSegmentAlreadyClosed
	}

	for i, meta := range metas {
		// Check the field map for other fields.
		b, exists := s.fields[meta.fieldHash]
		if !exists {
			// Close all fields.
			for _, f := range fields {
				f.Close()
			}
			return fmt.Errorf("field %v loaded but does not exist in segment field map", meta.retrieveOpts.fieldPath)
		}
		if b == nil {
			s.fields[meta.fieldHash] = fields[i]
			continue
		}
		fields[i].Close()
		fields[i] = b
	}

	s.status = segmentLoadedFromFS

	return nil
}

type loadFieldMetadata struct {
	retrieveOpts retrieveFieldOptions
	index        int
	fieldHash    hash.Hash
}

type queryFieldMeta struct {
	fieldPath           []string
	isTimestampField    *bool
	isRawDocSourceField *bool
	allowAllFieldTypes  bool
	allowedFieldTypes   map[field.ValueType]struct{}
}

// Precondition: m.fieldPath == other.fieldPath.
func (m *queryFieldMeta) MergeInPlace(other queryFieldMeta) {
	if other.isTimestampField != nil {
		m.isTimestampField = other.isTimestampField
	}
	if other.isRawDocSourceField != nil {
		m.isRawDocSourceField = other.isRawDocSourceField
	}
	if other.allowAllFieldTypes {
		return
	}
	m.allowAllFieldTypes = false
	if m.allowAllFieldTypes {
		m.allowedFieldTypes = other.allowedFieldTypes
		return
	}

	// Compute the intersection of allowed field types.
	allowedFieldTypesMap := make(map[field.ValueType]struct{})
	for ft := range other.allowedFieldTypes {
		_, exists := m.allowedFieldTypes[ft]
		if exists {
			allowedFieldTypesMap[ft] = struct{}{}
		}
	}
	m.allowedFieldTypes = allowedFieldTypesMap
}

func (s *immutableSeg) addQueryFieldToMap(
	fm map[hash.Hash]queryFieldMeta,
	newFieldMeta queryFieldMeta,
) error {
	// Do not insert empty fields.
	if len(newFieldMeta.fieldPath) == 0 {
		return nil
	}
	if !newFieldMeta.allowAllFieldTypes && len(newFieldMeta.allowedFieldTypes) == 0 {
		return errNoEligibleFieldTypesForQuery
	}
	fieldHash := s.fieldHashFn(newFieldMeta.fieldPath)
	meta, exists := fm[fieldHash]
	if !exists {
		fm[fieldHash] = newFieldMeta
		return nil
	}
	meta.MergeInPlace(newFieldMeta)
	if !meta.allowAllFieldTypes && len(meta.allowedFieldTypes) == 0 {
		return errNoEligibleFieldTypesForQuery
	}
	fm[fieldHash] = meta
	return nil
}
