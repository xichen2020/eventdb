package storage

import (
	"errors"
	"math"
	"sync"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/x/hash"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/m3db/m3x/context"
	"github.com/pborman/uuid"
)

const (
	defaultInitialNumFields = 64
)

var (
	// errSegmentIsFull is raised when writing to a segment whose document
	// count has reached the maximum threshold.
	errSegmentIsFull = errors.New("segment is full")

	// errSegmentAlreadySealed is raised when trying to write to a sealed segment.
	errSegmentAlreadySealed = errors.New("segment is already sealed")
)

// immutableDatabaseSegment is an immutable database segment.
type immutableDatabaseSegment interface {
	// ID returns the segment ID.
	ID() string

	// MinTimeNanos returns the earliest event timestamp in this segment.
	// If the segment is empty, this returns math.MaxInt64.
	MinTimeNanos() int64

	// MaxTimeNanos returns the latest event timestamp in this segment.
	// If the segment is empty, this returns math.MinInt64.
	MaxTimeNanos() int64

	// Intersects returns true if the time range associated with the events in
	// the segment intersects with the query time range.
	Intersects(startNanosInclusive, endNanosExclusive int64) bool

	// NumDocuments returns the number of documents (a.k.a. events) in this segment.
	NumDocuments() int32

	// IsFull returns true if the number of documents in the segment has reached
	// the maximum threshold.
	IsFull() bool

	// Flush flushes the segment to persistent storage.
	Flush(persistFns persist.Fns) error

	// QueryRaw returns raw results for a given query.
	QueryRaw(
		ctx context.Context,
		startNanosInclusive, endNanosExclusive int64,
		filters []query.FilterList,
		orderBy []query.OrderBy,
		limit *int,
	) (query.RawResult, error)

	// Close closes the segment.
	// The segment will not be closed until there's no reader on the segment.
	Close()
}

// mutableDatabaseSegment is a mutable database segment.
type mutableDatabaseSegment interface {
	immutableDatabaseSegment

	// Write writes an event to the mutable segment.
	Write(ev event.Event) error

	// Seal seals the segment and makes it unwriteable.
	Seal()
}

type dbSegment struct {
	sync.RWMutex

	id                    string
	opts                  *Options
	builderOpts           *document.FieldBuilderOptions
	timestampFieldPath    []string
	rawDocSourceFieldPath []string
	maxNumDocsPerSegment  int32

	// NB: We refer to an event containing a collection of fields a document
	// in conventional information retrieval terminology.
	sealed            bool
	closed            bool
	numDocs           int32
	minTimeNanos      int64
	maxTimeNanos      int64
	timestampField    document.DocsFieldBuilder
	rawDocSourceField document.DocsFieldBuilder
	fields            map[hash.Hash]document.DocsFieldBuilder
	fieldBuf          []document.DocsField
}

func newDatabaseSegment(
	opts *Options,
) *dbSegment {
	builderOpts := document.NewFieldBuilderOptions().
		SetBoolArrayPool(opts.BoolArrayPool()).
		SetIntArrayPool(opts.IntArrayPool()).
		SetDoubleArrayPool(opts.DoubleArrayPool()).
		SetStringArrayPool(opts.StringArrayPool()).
		SetInt64ArrayPool(opts.Int64ArrayPool())

	mandatoryFieldBuilderOpts := builderOpts.SetIsMandatoryField(true)
	timestampFieldPath := []string{opts.TimestampFieldName()}
	timestampFieldBuilder := document.NewFieldBuilder(timestampFieldPath, mandatoryFieldBuilderOpts)
	rawDocSourceFieldPath := []string{opts.RawDocSourceFieldName()}
	rawDocSourceFieldBuilder := document.NewFieldBuilder(rawDocSourceFieldPath, mandatoryFieldBuilderOpts)

	return &dbSegment{
		id:                    uuid.New(),
		opts:                  opts,
		builderOpts:           builderOpts,
		timestampFieldPath:    timestampFieldPath,
		rawDocSourceFieldPath: rawDocSourceFieldPath,
		maxNumDocsPerSegment:  opts.MaxNumDocsPerSegment(),
		minTimeNanos:          math.MaxInt64,
		maxTimeNanos:          math.MinInt64,
		timestampField:        timestampFieldBuilder,
		rawDocSourceField:     rawDocSourceFieldBuilder,
		fields:                make(map[hash.Hash]document.DocsFieldBuilder, defaultInitialNumFields),
	}
}

func (s *dbSegment) ID() string { return s.id }

func (s *dbSegment) MinTimeNanos() int64 {
	s.RLock()
	minTimeNanos := s.minTimeNanos
	s.RUnlock()
	return minTimeNanos
}

func (s *dbSegment) MaxTimeNanos() int64 {
	s.RLock()
	maxTimeNanos := s.maxTimeNanos
	s.RUnlock()
	return maxTimeNanos
}

func (s *dbSegment) Intersects(startNanosInclusive, endNanosExclusive int64) bool {
	s.RLock()
	if s.numDocs == 0 {
		s.RUnlock()
		return false
	}
	hasIntersection := s.minTimeNanos < endNanosExclusive && s.maxTimeNanos >= startNanosInclusive
	s.RUnlock()
	return hasIntersection
}

func (s *dbSegment) NumDocuments() int32 {
	s.RLock()
	numDocs := s.numDocs
	s.RUnlock()
	return numDocs
}

func (s *dbSegment) IsFull() bool {
	return s.NumDocuments() == s.maxNumDocsPerSegment
}

func (s *dbSegment) QueryRaw(
	ctx context.Context,
	startNanosInclusive, endNanosExclusive int64,
	filters []query.FilterList,
	orderBy []query.OrderBy,
	limit *int,
) (query.RawResult, error) {
	return query.RawResult{}, errors.New("not implemented")
}

func (s *dbSegment) Flush(persistFns persist.Fns) error {
	s.RLock()
	defer s.RUnlock()

	// NB: The field buffer is only used by the flushing thread in a single-thread
	// context so it doesn't need to be protected by a write lock.
	numDocs := int(s.numDocs)
	if numFields := 2 + len(s.fields); cap(s.fieldBuf) < numFields {
		s.fieldBuf = make([]document.DocsField, 0, numFields)
	} else {
		s.fieldBuf = s.fieldBuf[:0]
	}
	s.fieldBuf = append(s.fieldBuf, s.timestampField.Build(numDocs))
	s.fieldBuf = append(s.fieldBuf, s.rawDocSourceField.Build(numDocs))
	for _, f := range s.fields {
		s.fieldBuf = append(s.fieldBuf, f.Build(numDocs))
	}

	return persistFns.WriteFields(s.fieldBuf)
}

func (s *dbSegment) Write(ev event.Event) error {
	s.Lock()
	if s.sealed {
		s.Unlock()
		return errSegmentAlreadySealed
	}
	if s.numDocs == s.maxNumDocsPerSegment {
		s.Unlock()
		return errSegmentIsFull
	}
	if s.minTimeNanos > ev.TimeNanos {
		s.minTimeNanos = ev.TimeNanos
	}
	if s.maxTimeNanos < ev.TimeNanos {
		s.maxTimeNanos = ev.TimeNanos
	}
	docID := s.numDocs
	s.numDocs++

	// Write event fields.
	s.writeTimestampFieldWithLock(docID, ev.TimeNanos)
	s.writeRawDocSourceFieldWithLock(docID, ev.RawData)
	for ev.FieldIter.Next() {
		f := ev.FieldIter.Current()
		// We store timestamp field separately.
		if len(f.Path) == 1 && f.Path[0] == s.opts.TimestampFieldName() {
			continue
		}
		b := s.getOrInsertWithLock(f.Path, s.builderOpts)
		b.Add(docID, f.Value)
	}
	ev.FieldIter.Close()

	s.Unlock()
	return nil
}

func (s *dbSegment) Seal() {
	s.Lock()
	s.sealed = true
	s.Unlock()
}

func (s *dbSegment) Close() {
	s.Lock()
	if s.closed {
		s.Unlock()
		return
	}
	// NB: Always sealing the document before closing.
	s.sealed = true
	s.closed = true
	s.Unlock()

	s.timestampFieldPath = nil
	s.rawDocSourceFieldPath = nil
	s.timestampField.Close()
	s.timestampField = nil
	s.rawDocSourceField.Close()
	s.rawDocSourceField = nil
	for _, f := range s.fields {
		f.Close()
	}
	s.fields = nil
}

func (s *dbSegment) writeTimestampFieldWithLock(docID int32, val int64) {
	v := field.ValueUnion{
		Type:         field.TimeType,
		TimeNanosVal: val,
	}
	s.timestampField.Add(docID, v)
}

func (s *dbSegment) writeRawDocSourceFieldWithLock(docID int32, val []byte) {
	v := field.ValueUnion{
		Type:      field.StringType,
		StringVal: unsafe.ToString(val),
	}
	s.rawDocSourceField.Add(docID, v)
}

func (s *dbSegment) getOrInsertWithLock(
	fieldPath []string,
	builderOpts *document.FieldBuilderOptions,
) document.DocsFieldBuilder {
	pathHash := hash.StringArrayHash(fieldPath, s.opts.FieldPathSeparator())
	if b, exists := s.fields[pathHash]; exists {
		return b
	}
	b := document.NewFieldBuilder(fieldPath, builderOpts)
	s.fields[pathHash] = b
	return b
}
