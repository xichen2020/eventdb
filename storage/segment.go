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
	// errSegmentIsFull is raised when writing to a segment whose document
	// count has reached the maximum threshold.
	errSegmentIsFull = errors.New("segment is full")

	// errSegmentAlreadySealed is raised when trying to mutabe a sealed segment.
	errSegmentAlreadySealed = errors.New("segment is already sealed")
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

	// IncReader increments the number of readers reading the segment.
	// This prevents the segment from closing until all readers have finished reading.
	IncReader()

	// DecReader decrements the number of readers reading the segment.
	// This should be called in pair with `IncReader`.
	DecReader()

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

	id                   string
	opts                 *Options
	maxNumDocsPerSegment int32
	int64ArrayPool       *pool.BucketizedInt64ArrayPool
	stringArrayPool      *pool.BucketizedStringArrayPool

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
	wgRead       sync.WaitGroup
}

func newDatabaseSegment(
	opts *Options,
) *dbSegment {
	int64ArrayPool := opts.Int64ArrayPool()
	stringArrayPool := opts.StringArrayPool()
	return &dbSegment{
		id:                   uuid.New(),
		opts:                 opts,
		maxNumDocsPerSegment: opts.MaxNumDocsPerSegment(),
		int64ArrayPool:       int64ArrayPool,
		stringArrayPool:      stringArrayPool,
		timeNanos:            int64ArrayPool.Get(defaultInitialNumDocs),
		rawDocs:              stringArrayPool.Get(defaultInitialNumDocs),
		fields:               make(map[hash.Hash]*fieldDocValues, defaultInitialNumFields),
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

func (s *dbSegment) IncReader() { s.wgRead.Add(1) }

func (s *dbSegment) DecReader() { s.wgRead.Done() }

func (s *dbSegment) Flush(persistFns persist.Fns) error {
	s.RLock()
	defer s.RUnlock()

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

func (s *dbSegment) Close() {
	s.Lock()
	if s.closed {
		s.Unlock()
		return
	}
	// Always sealing the document before closing.
	s.sealed = true
	s.closed = true
	s.Unlock()

	// Wait for all readers to finish. This will block if there are existing
	// pending read operations.
	s.wgRead.Wait()
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
}
