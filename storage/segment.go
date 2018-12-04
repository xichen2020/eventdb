package storage

import (
	"errors"
	"sync"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/x/hash"
	"github.com/xichen2020/eventdb/x/pool"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/pborman/uuid"
)

const (
	defaultInitialNumDocs   = 4096
	defaultInitialNumFields = 64
)

var (
	// errSegmentAlreadySealed is raised when trying to mutabe a sealed segment.
	errSegmentAlreadySealed = errors.New("segment is already sealed")

	// errSegmentAlreadyClosed is raisd when trying to close a segment that's already closed.
	errSegmentAlreadyClosed = errors.New("segment is already closed")
)

// immutableDatabaseSegment is an immutable database segment.
type immutableDatabaseSegment interface {
	// ID returns the segment ID.
	ID() string

	// MinTimeNanos returns the earliest event timestamp in this segment.
	// If the segment is empty, this returns 0.
	MinTimeNanos() int64

	// MaxTimeNanos returns the latest event timestamp in this segment.
	// If the segment is empty, this returns 0.
	MaxTimeNanos() int64

	// NumDocuments returns the number of documents (a.k.a. events) in this segment.
	NumDocuments() int32

	// Flush flushes the immutable segment to persistent storage.
	Flush(persistFns persist.Fns) error
}

// mutableDatabaseSegment is a mutable database segment.
type mutableDatabaseSegment interface {
	immutableDatabaseSegment

	// Write writes an event to the mutable segment.
	Write(ev event.Event) error

	// Seal seals the mutable segment and makes it immutable.
	Seal()

	// Close closes the mutable segment.
	Close() error
}

// TODO(xichen): Pool timestamp array and raw docs byte array.
// TODO(xichen): Treat tiemstamp and rawDocs as normal fields.
type dbSegment struct {
	sync.RWMutex

	id              string
	opts            *Options
	int64ArrayPool  *pool.BucketizedInt64ArrayPool
	stringArrayPool *pool.BucketizedStringArrayPool

	// NB: We refer to an event containing a collection of fields a document
	// in conventional information retrieval terminology.
	sealed       bool
	closed       bool
	numDocs      int32
	minTimeNanos int64
	maxTimeNanos int64
	timeNanos    []int64
	rawDocs      []string
	fields       map[hash.Hash]*fieldDocValues
}

func newDatabaseSegment(
	opts *Options,
) *dbSegment {
	int64ArrayPool := opts.Int64ArrayPool()
	stringArrayPool := opts.StringArrayPool()
	return &dbSegment{
		id:              uuid.New(),
		opts:            opts,
		int64ArrayPool:  int64ArrayPool,
		stringArrayPool: stringArrayPool,
		timeNanos:       int64ArrayPool.Get(defaultInitialNumDocs),
		rawDocs:         stringArrayPool.Get(defaultInitialNumDocs),
		fields:          make(map[hash.Hash]*fieldDocValues, defaultInitialNumFields),
	}
}

func (s *dbSegment) ID() string { return s.id }

func (s *dbSegment) MinTimeNanos() int64 { return s.minTimeNanos }

func (s *dbSegment) MaxTimeNanos() int64 { return s.maxTimeNanos }

func (s *dbSegment) NumDocuments() int32 { return s.numDocs }

func (s *dbSegment) Write(ev event.Event) error {
	s.Lock()
	if s.sealed {
		s.Unlock()
		return errSegmentAlreadySealed
	}
	if s.minTimeNanos > ev.TimeNanos {
		s.minTimeNanos = ev.TimeNanos
	}
	if s.maxTimeNanos < ev.TimeNanos {
		s.maxTimeNanos = ev.TimeNanos
	}
	docID := s.numDocs
	s.numDocs++
	s.timeNanos = pool.AppendInt64(s.timeNanos, ev.TimeNanos, s.int64ArrayPool)
	s.rawDocs = pool.AppendString(s.rawDocs, unsafe.ToString(ev.RawData), s.stringArrayPool)

	for ev.FieldIter.Next() {
		f := ev.FieldIter.Current()
		// We store timestamp field separately.
		if len(f.Path) == 1 && f.Path[0] == s.opts.TimestampFieldName() {
			continue
		}
		pathHash := hash.StringArrayHash(f.Path, s.opts.FieldPathSeparator())
		w, exists := s.fields[pathHash]
		if !exists {
			w = newFieldDocValues(f.Path, s.opts)
			s.fields[pathHash] = w
		}
		w.addValue(docID, f.Value)
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

func (s *dbSegment) Flush(persistFns persist.Fns) error {
	if err := persistFns.WriteTimestamps(s.timeNanos); err != nil {
		return err
	}
	if err := persistFns.WriteRawDocs(s.rawDocs); err != nil {
		return err
	}
	for _, fw := range s.fields {
		// If we encounter an error when persisting a single field, don't continue
		// as the file on disk could be in a corrupt state.
		if err := fw.flush(persistFns); err != nil {
			return err
		}
	}
	return nil
}

func (s *dbSegment) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errSegmentAlreadyClosed
	}
	// Always sealing the document before closing.
	s.sealed = true
	s.closed = true
	s.timeNanos = s.timeNanos[:0]
	s.int64ArrayPool.Put(s.timeNanos, cap(s.timeNanos))
	s.timeNanos = nil
	s.rawDocs = s.rawDocs[:0]
	s.stringArrayPool.Put(s.rawDocs, cap(s.rawDocs))
	s.rawDocs = nil
	for _, f := range s.fields {
		f.close()
	}
	s.fields = nil
	return nil
}
