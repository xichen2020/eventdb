package storage

import (
	"errors"
	"math"
	"sync"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/x/hash"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/m3db/m3x/context"
)

type mutableSegment interface {
	immutableSegmentBase

	// QueryRaw returns results for a given raw query.
	QueryRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
	) (*query.RawResults, error)

	// QueryGrouped returns results for a given grouped query.
	QueryGrouped(
		ctx context.Context,
		q query.ParsedGroupedQuery,
	) (*query.GroupedResults, error)

	// IsFull returns true if the number of documents in the segment has reached
	// the maximum threshold.
	IsFull() bool

	// Write writes an document to the mutable segment.
	Write(doc document.Document) error

	// Seal seals and closes the mutable segment and returns an immutable segment.
	Seal() (immutableSegment, error)
}

const (
	defaultInitialNumFields = 64
)

var (
	// errMutableSegmentAlreadyFull is raised when writing to a mutable segment whose document
	// count has reached the maximum threshold.
	errMutableSegmentAlreadyFull = errors.New("mutable segment is already full")

	// errMutableSegmentAlreadySealed is raised when trying to perform an operation against a sealed mutable segment.
	errMutableSegmentAlreadySealed = errors.New("mutable segment is already closed")

	// errMutableSegmentAlreadyClosed is raised when trying to write to a closed mutable segment.
	errMutableSegmentAlreadyClosed = errors.New("mutable segment is already closed")

	// errFieldPathConflictsWithRawDocSourcePath is raised when the name of the encountered field conflicts w/ the configured raw doc source path name.
	errFieldPathConflictsWithRawDocSourcePath = errors.New("field path conflicts w/ raw doc source path")
)

type isSpecialFieldFn func(fieldPath []string) bool

type mutableSeg struct {
	sync.RWMutex
	mutableSegmentBase

	namespace             []byte
	shard                 uint32
	opts                  *Options
	builderOpts           *indexfield.DocsFieldBuilderOptions
	isTimestampFieldFn    isSpecialFieldFn
	isRawDocSourceFieldFn isSpecialFieldFn
	fieldHashFn           hash.StringArrayHashFn
	fieldRetriever        persist.FieldRetriever
	maxNumDocsPerSegment  int32

	sealed bool
	closed bool
	fields map[hash.Hash]indexfield.DocsFieldBuilder
	// These two builders provide fast access to builders for the timestamp field
	// and the raw doc source field which are present in every index.
	// TODO(xichen): Use full doc ID set builder for these two fields.
	timestampField    indexfield.DocsFieldBuilder
	rawDocSourceField indexfield.DocsFieldBuilder
}

func newMutableSegment(
	namespace []byte,
	shard uint32,
	id string,
	opts *Options,
) *mutableSeg {
	builderOpts := indexfield.NewDocsFieldBuilderOptions().
		SetBoolArrayPool(opts.BoolArrayPool()).
		SetIntArrayPool(opts.IntArrayPool()).
		SetDoubleArrayPool(opts.DoubleArrayPool()).
		SetStringArrayPool(opts.StringArrayPool()).
		SetInt64ArrayPool(opts.Int64ArrayPool())

	fieldHashFn := opts.FieldHashFn()
	timestampFieldPath := opts.TimestampFieldPath()
	isTimestampFieldFn := func(fieldPath []string) bool {
		if len(fieldPath) != len(timestampFieldPath) {
			return false
		}
		for i := 0; i < len(fieldPath); i++ {
			if fieldPath[i] != timestampFieldPath[i] {
				return false
			}
		}
		return true
	}
	timestampFieldBuilder := indexfield.NewDocsFieldBuilder(timestampFieldPath, builderOpts)
	timestampFieldHash := fieldHashFn(timestampFieldPath)

	rawDocSourceFieldPath := opts.RawDocSourceFieldPath()
	isRawDocSourceFieldFn := func(fieldPath []string) bool {
		if len(fieldPath) != len(rawDocSourceFieldPath) {
			return false
		}
		for i := 0; i < len(fieldPath); i++ {
			if fieldPath[i] != rawDocSourceFieldPath[i] {
				return false
			}
		}
		return true
	}
	rawDocSourceFieldBuilder := indexfield.NewDocsFieldBuilder(rawDocSourceFieldPath, builderOpts)
	rawDocSourceFieldHash := fieldHashFn(rawDocSourceFieldPath)

	fields := make(map[hash.Hash]indexfield.DocsFieldBuilder, defaultInitialNumFields)
	fields[timestampFieldHash] = timestampFieldBuilder
	fields[rawDocSourceFieldHash] = rawDocSourceFieldBuilder

	return &mutableSeg{
		mutableSegmentBase:    newBaseSegment(id, 0, math.MaxInt64, math.MinInt64),
		namespace:             namespace,
		shard:                 shard,
		opts:                  opts,
		builderOpts:           builderOpts,
		isTimestampFieldFn:    isTimestampFieldFn,
		isRawDocSourceFieldFn: isRawDocSourceFieldFn,
		fieldHashFn:           fieldHashFn,
		fieldRetriever:        opts.FieldRetriever(),
		maxNumDocsPerSegment:  opts.MaxNumDocsPerSegment(),
		timestampField:        timestampFieldBuilder,
		rawDocSourceField:     rawDocSourceFieldBuilder,
		fields:                fields,
	}
}

func (s *mutableSeg) ID() string { return s.mutableSegmentBase.ID() }

func (s *mutableSeg) NumDocuments() int32 {
	s.RLock()
	numDocs := s.mutableSegmentBase.NumDocuments()
	s.RUnlock()
	return numDocs
}

func (s *mutableSeg) MinTimeNanos() int64 {
	s.RLock()
	minTimeNanos := s.mutableSegmentBase.MinTimeNanos()
	s.RUnlock()
	return minTimeNanos
}

func (s *mutableSeg) MaxTimeNanos() int64 {
	s.RLock()
	maxTimeNanos := s.mutableSegmentBase.MaxTimeNanos()
	s.RUnlock()
	return maxTimeNanos
}

func (s *mutableSeg) Intersects(startNanosInclusive, endNanosExclusive int64) bool {
	s.RLock()
	intersects := s.mutableSegmentBase.Intersects(startNanosInclusive, endNanosExclusive)
	s.RUnlock()
	return intersects
}

func (s *mutableSeg) QueryRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
) (*query.RawResults, error) {
	// Fast path if the limit indicates no results are needed.
	if q.Limit <= 0 {
		return nil, nil
	}

	// Retrieve the fields.
	s.RLock()
	if s.closed {
		s.RUnlock()
		return nil, errMutableSegmentAlreadyClosed
	}
	if s.sealed {
		s.RUnlock()
		return nil, errMutableSegmentAlreadySealed
	}

	allowedFieldTypes, fieldIndexMap, queryFields, err := s.collectFieldsForRawQueryWithLock(q)
	if err != nil {
		s.RUnlock()
		return nil, err
	}

	numDocuments := s.mutableSegmentBase.NumDocuments()
	s.RUnlock()

	defer func() {
		for i := range queryFields {
			if queryFields[i] != nil {
				queryFields[i].Close()
				queryFields[i] = nil
			}
		}
	}()

	// Validate that the fields to order results by have one and only one field type.
	hasEmptyResult, err := validateOrderByClauses(allowedFieldTypes, q.OrderBy)
	if err != nil {
		return nil, err
	}
	if hasEmptyResult {
		return nil, nil
	}

	if queryFields[rawDocSourceFieldIdx] == nil {
		return nil, errNoRawDocSourceField
	}
	rawDocSourceField, ok := queryFields[rawDocSourceFieldIdx].StringField()
	if !ok {
		return nil, errNoStringValuesInRawDocSourceField
	}

	// Apply filters to determine the doc ID set matching the filters.
	filteredDocIDIter, err := applyFilters(
		q.StartNanosInclusive, q.EndNanosExclusive, q.Filters,
		allowedFieldTypes, fieldIndexMap, queryFields, numDocuments,
	)
	if err != nil {
		return nil, err
	}

	rawResults := q.NewRawResults()
	err = collectOrderedRawResults(
		allowedFieldTypes,
		fieldIndexMap,
		queryFields,
		rawDocSourceField,
		filteredDocIDIter,
		q,
		rawResults,
	)
	if err != nil {
		return nil, err
	}
	return rawResults, nil
}

func (s *mutableSeg) QueryGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
) (*query.GroupedResults, error) {
	return nil, errors.New("not implemented")
}

func (s *mutableSeg) IsFull() bool {
	return s.NumDocuments() == s.maxNumDocsPerSegment
}

func (s *mutableSeg) Write(doc document.Document) error {
	s.Lock()
	if s.closed {
		s.Unlock()
		return errMutableSegmentAlreadyClosed
	}
	if s.sealed {
		s.Unlock()
		return errMutableSegmentAlreadySealed
	}

	// Determine document ID.
	numDocs := s.mutableSegmentBase.NumDocuments()
	if numDocs == s.maxNumDocsPerSegment {
		s.Unlock()
		return errMutableSegmentAlreadyFull
	}
	docID := numDocs
	s.mutableSegmentBase.SetNumDocuments(docID)

	// Update timestamps.
	minTimeNanos := s.mutableSegmentBase.MinTimeNanos()
	if minTimeNanos > doc.TimeNanos {
		s.mutableSegmentBase.SetMinTimeNanos(doc.TimeNanos)
	}
	maxTimeNanos := s.mutableSegmentBase.MaxTimeNanos()
	if maxTimeNanos < doc.TimeNanos {
		s.mutableSegmentBase.SetMaxTimeNanos(doc.TimeNanos)
	}

	// Write document fields.
	s.writeRawDocSourceFieldWithLock(docID, doc.RawData)
	for doc.FieldIter.Next() {
		f := doc.FieldIter.Current()
		// We store timestamp field as a time value.
		if s.isTimestampFieldFn(f.Path) {
			s.writeTimestampFieldWithLock(docID, doc.TimeNanos)
			continue
		}
		if s.isRawDocSourceFieldFn(f.Path) {
			return errFieldPathConflictsWithRawDocSourcePath
		}
		b := s.getOrInsertWithLock(f.Path, s.builderOpts)
		b.Add(docID, f.Value)
	}
	doc.FieldIter.Close()

	s.Unlock()
	return nil
}

func (s *mutableSeg) Seal() (immutableSegment, error) {
	s.Lock()
	if s.closed {
		s.Unlock()
		return nil, errMutableSegmentAlreadyClosed
	}
	if s.sealed {
		s.Unlock()
		return nil, errMutableSegmentAlreadySealed
	}

	numDocs := s.mutableSegmentBase.NumDocuments()
	fields := make(map[hash.Hash]indexfield.DocsField)
	for k, b := range s.fields {
		fields[k] = b.Seal(numDocs)
	}

	var (
		id           = s.mutableSegmentBase.ID()
		minTimeNanos = s.mutableSegmentBase.MinTimeNanos()
		maxTimeNanos = s.mutableSegmentBase.MaxTimeNanos()
		opts         = immutableSegmentOptions{
			fieldHashFn:    s.fieldHashFn,
			fieldRetriever: s.fieldRetriever,
		}
	)
	res := newImmutableSegment(
		s.namespace, s.shard, id, numDocs, minTimeNanos, maxTimeNanos,
		segmentFullyLoaded, inMemoryOnly, fields, opts,
	)

	// NB: There is no need to wait for all readers to finish here because
	// the readers can still operate on the field snapshots independently
	// after the field builders are sealed.
	s.timestampField = nil
	s.rawDocSourceField = nil
	s.fields = nil
	s.sealed = true

	s.Unlock()
	return res, nil
}

func (s *mutableSeg) Close() {
	// Wait for all readers to finish.
	s.mutableSegmentBase.Close()

	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	s.timestampField = nil
	s.rawDocSourceField = nil
	for _, b := range s.fields {
		b.Close()
	}
	s.fields = nil
}

func (s *mutableSeg) collectFieldsForRawQueryWithLock(
	q query.ParsedRawQuery,
) (
	fieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	queryFields []indexfield.DocsField,
	err error,
) {
	numFieldsForQuery := q.NumFieldsForQuery()
	fieldTypes = make([]field.ValueTypeSet, numFieldsForQuery)
	fieldIndexMap = make([]int, numFieldsForQuery)
	queryFields = make([]indexfield.DocsField, 0, numFieldsForQuery)

	for fieldHash, fm := range q.FieldConstraints {
		builder, exists := s.fields[fieldHash]
		if !exists {
			// Field does not exist.
			queryFields = append(queryFields, nil)
			for sourceIdx := range fm.AllowedTypesBySourceIdx {
				fieldIndexMap[sourceIdx] = len(queryFields) - 1
			}
			continue
		}
		var fieldTypeSet field.ValueTypeSet
		if len(fm.AllowedTypesBySourceIdx) <= 1 {
			for _, ft := range fm.AllowedTypesBySourceIdx {
				fieldTypeSet = ft
				break
			}
		} else {
			fieldTypeSet = make(field.ValueTypeSet, field.NumValidFieldTypes)
			for _, ft := range fm.AllowedTypesBySourceIdx {
				fieldTypeSet.MergeInPlace(ft)
			}
		}
		docsField, _, err := builder.SnapshotFor(fieldTypeSet)
		if err != nil {
			for i := range queryFields {
				if queryFields[i] != nil {
					queryFields[i].Close()
					queryFields[i] = nil
				}
			}
			return nil, nil, nil, err
		}
		availableFieldTypes := docsField.Metadata().FieldTypes
		queryFields = append(queryFields, docsField)
		for sourceIdx, ft := range fm.AllowedTypesBySourceIdx {
			fieldTypes[sourceIdx] = intersectFieldTypes(availableFieldTypes, ft)
			fieldIndexMap[sourceIdx] = len(queryFields) - 1
		}
	}
	return fieldTypes, fieldIndexMap, queryFields, nil
}

func (s *mutableSeg) writeTimestampFieldWithLock(docID int32, val int64) {
	v := field.ValueUnion{
		Type:         field.TimeType,
		TimeNanosVal: val,
	}
	s.timestampField.Add(docID, v)
}

func (s *mutableSeg) writeRawDocSourceFieldWithLock(docID int32, val []byte) {
	v := field.ValueUnion{
		Type:      field.StringType,
		StringVal: unsafe.ToString(val),
	}
	s.rawDocSourceField.Add(docID, v)
}

func (s *mutableSeg) getOrInsertWithLock(
	fieldPath []string,
	builderOpts *indexfield.DocsFieldBuilderOptions,
) indexfield.DocsFieldBuilder {
	pathHash := s.fieldHashFn(fieldPath)
	if b, exists := s.fields[pathHash]; exists {
		return b
	}
	// Clone the field path since it could change as we iterate.
	clonedPath := make([]string, len(fieldPath))
	copy(clonedPath, fieldPath)
	b := indexfield.NewDocsFieldBuilder(clonedPath, builderOpts)
	s.fields[pathHash] = b
	return b
}
