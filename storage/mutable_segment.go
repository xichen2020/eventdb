package storage

import (
	"errors"
	"math"
	"sync"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/index"
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
		startNanosInclusive, endNanosExclusive int64,
		filters []query.FilterList,
		orderBy []query.OrderBy,
		limit *int,
	) (query.RawResult, error)

	// IsFull returns true if the number of documents in the segment has reached
	// the maximum threshold.
	IsFull() bool

	// Write writes an event to the mutable segment.
	Write(ev event.Event) error

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

	// errMutableSegmentAlreadyClosed is raised when trying to write to a closed mutable segment.
	errMutableSegmentAlreadyClosed = errors.New("mutable segment is already closed")
)

type isSpecialFieldFn func(fieldPath []string) bool

type fieldHashFn func(fieldPath []string) hash.Hash

// retrieveFieldFromFSFn retrieves field from filesystem.
type retrieveFieldFromFSFn func(opts retrieveFieldOptions) (index.DocsField, error)

type mutableSeg struct {
	sync.RWMutex
	mutableSegmentBase

	opts                  *Options
	builderOpts           *index.FieldBuilderOptions
	isTimestampFieldFn    isSpecialFieldFn
	fieldHashFn           fieldHashFn
	retrieveFieldFromFSFn retrieveFieldFromFSFn
	maxNumDocsPerSegment  int32

	closed bool
	fields map[hash.Hash]index.DocsFieldBuilder
	// These two builders provide fast access to builders for the timestamp field
	// and the raw doc source field which are present in every index.
	timestampField    index.DocsFieldBuilder
	rawDocSourceField index.DocsFieldBuilder
}

func newMutableSegment(id string, opts *Options) *mutableSeg {
	builderOpts := index.NewFieldBuilderOptions().
		SetBoolArrayPool(opts.BoolArrayPool()).
		SetIntArrayPool(opts.IntArrayPool()).
		SetDoubleArrayPool(opts.DoubleArrayPool()).
		SetStringArrayPool(opts.StringArrayPool()).
		SetInt64ArrayPool(opts.Int64ArrayPool())

	// TODO(xichen): Make this part of storage options.
	fieldHashFn := func(fieldPath []string) hash.Hash {
		return hash.StringArrayHash(fieldPath, opts.FieldPathSeparator())
	}

	timestampFieldPath := []string{opts.TimestampFieldName()}
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
	timestampFieldBuilder := index.NewFieldBuilder(timestampFieldPath, builderOpts)
	timestampFieldHash := fieldHashFn(timestampFieldPath)

	rawDocSourceFieldPath := []string{opts.RawDocSourceFieldName()}
	rawDocSourceFieldBuilder := index.NewFieldBuilder(rawDocSourceFieldPath, builderOpts)
	rawDocSourceFieldHash := fieldHashFn(rawDocSourceFieldPath)

	fields := make(map[hash.Hash]index.DocsFieldBuilder, defaultInitialNumFields)
	fields[timestampFieldHash] = timestampFieldBuilder
	fields[rawDocSourceFieldHash] = rawDocSourceFieldBuilder

	return &mutableSeg{
		mutableSegmentBase:    newBaseSegment(id, 0, math.MaxInt64, math.MinInt64),
		opts:                  opts,
		builderOpts:           builderOpts,
		isTimestampFieldFn:    isTimestampFieldFn,
		fieldHashFn:           fieldHashFn,
		retrieveFieldFromFSFn: nil, // TODO(xichen): Initialize this properly
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

// TODO(xichen): Implement this.
func (s *mutableSeg) QueryRaw(
	ctx context.Context,
	startNanosInclusive, endNanosExclusive int64,
	filters []query.FilterList,
	orderBy []query.OrderBy,
	limit *int,
) (query.RawResult, error) {
	return query.RawResult{}, errors.New("not implemented")
}

func (s *mutableSeg) IsFull() bool {
	return s.NumDocuments() == s.maxNumDocsPerSegment
}

func (s *mutableSeg) Write(ev event.Event) error {
	s.Lock()
	if s.closed {
		s.Unlock()
		return errMutableSegmentAlreadyClosed
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
	if minTimeNanos > ev.TimeNanos {
		s.mutableSegmentBase.SetMinTimeNanos(ev.TimeNanos)
	}
	maxTimeNanos := s.mutableSegmentBase.MaxTimeNanos()
	if maxTimeNanos < ev.TimeNanos {
		s.mutableSegmentBase.SetMaxTimeNanos(ev.TimeNanos)
	}

	// Write event fields.
	s.writeRawDocSourceFieldWithLock(docID, ev.RawData)
	for ev.FieldIter.Next() {
		f := ev.FieldIter.Current()
		// We store timestamp field as a time value.
		if s.isTimestampFieldFn(f.Path) {
			s.writeTimestampFieldWithLock(docID, ev.TimeNanos)
			continue
		}
		b := s.getOrInsertWithLock(f.Path, s.builderOpts)
		b.Add(docID, f.Value)
	}
	ev.FieldIter.Close()

	s.Unlock()
	return nil
}

func (s *mutableSeg) Seal() (immutableSegment, error) {
	s.Lock()
	if s.closed {
		s.Unlock()
		return nil, errMutableSegmentAlreadyClosed
	}

	numDocs := s.mutableSegmentBase.NumDocuments()
	fields := make(map[hash.Hash]index.DocsField)
	for k, b := range s.fields {
		fields[k] = b.Seal(numDocs)
	}

	var (
		id           = s.mutableSegmentBase.ID()
		minTimeNanos = s.mutableSegmentBase.MinTimeNanos()
		maxTimeNanos = s.mutableSegmentBase.MaxTimeNanos()
		opts         = immutableSegmentOptions{
			fieldHashFn:           s.fieldHashFn,
			retrieveFieldFromFSFn: s.retrieveFieldFromFSFn,
		}
	)
	res := newImmutableSegment(
		id, numDocs, minTimeNanos, maxTimeNanos,
		segmentLoadedFullyInMem, fields, opts,
	)

	// NB: There is no need to wait for all readers to finish here because
	// the readers can still operate on the field snapshots independently
	// after the field builders are sealed.
	s.timestampField = nil
	s.rawDocSourceField = nil
	s.fields = nil
	s.closeWithLock()

	s.Unlock()
	return res, nil
}

func (s *mutableSeg) Close() {
	// Wait for all readers to finish.
	s.mutableSegmentBase.Close()

	s.Lock()
	s.closeWithLock()
	s.Unlock()
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
	builderOpts *index.FieldBuilderOptions,
) index.DocsFieldBuilder {
	pathHash := hash.StringArrayHash(fieldPath, s.opts.FieldPathSeparator())
	if b, exists := s.fields[pathHash]; exists {
		return b
	}
	b := index.NewFieldBuilder(fieldPath, builderOpts)
	s.fields[pathHash] = b
	return b
}

func (s *mutableSeg) closeWithLock() {
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
