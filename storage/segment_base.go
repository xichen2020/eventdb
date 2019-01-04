package storage

import (
	"sync"
	"sync/atomic"
)

type immutableSegmentBase interface {
	// ID returns the segment ID.
	ID() string

	// NumDocuments returns the number of documents in this segment.
	NumDocuments() int32

	// MinTimeNanos returns the earliest document timestamp in this segment.
	MinTimeNanos() int64

	// MaxTimeNanos returns the latest document timestamp in this segment.
	MaxTimeNanos() int64

	// Intersects returns true if the time range associated with the documents in
	// the segment intersects with the query time range. If the segment is empty,
	// this returns false.
	Intersects(startNanosInclusive, endNanosExclusive int64) bool

	// IncAccessor increments the number of accessors accessing the segment.
	IncAccessor()

	// DecAccessor decrements the number of accessors accessing the segment.
	DecAccessor()

	// NumAccessors returns the number of accessors accessing the segment.
	NumAccessors() int

	// Close closes the segment.
	Close()
}

type mutableSegmentBase interface {
	immutableSegmentBase

	// SetNumDocuments sets the number of documents in this segment.
	SetNumDocuments(v int32)

	// SetMinTimeNanos sets the earliest document timestamp in this segment.
	SetMinTimeNanos(v int64)

	// SetMaxTimeNanos sets the latest document timestamp in this segment.
	SetMaxTimeNanos(v int64)
}

// baseSegment contains the base segment metadata.
// NB: baseSegment is not thread-safe. Concurrent access to base segment is protected externally.
type baseSegment struct {
	id           string
	numDocs      int32
	minTimeNanos int64
	maxTimeNanos int64
	numAccessors int32
	wgAccess     sync.WaitGroup
}

func newBaseSegment(
	id string,
	numDocs int32,
	minTimeNanos int64,
	maxTimeNanos int64,
) *baseSegment {
	return &baseSegment{
		id:           id,
		numDocs:      numDocs,
		minTimeNanos: minTimeNanos,
		maxTimeNanos: maxTimeNanos,
	}
}

// Immutable segment APIs.
func (s *baseSegment) ID() string          { return s.id }
func (s *baseSegment) NumDocuments() int32 { return s.numDocs }
func (s *baseSegment) MinTimeNanos() int64 { return s.minTimeNanos }
func (s *baseSegment) MaxTimeNanos() int64 { return s.maxTimeNanos }
func (s *baseSegment) Intersects(startNanosInclusive, endNanosExclusive int64) bool {
	if s.numDocs == 0 {
		return false
	}
	return s.minTimeNanos < endNanosExclusive && s.maxTimeNanos >= startNanosInclusive
}

// Readable segment APIs.
func (s *baseSegment) IncAccessor() {
	atomic.AddInt32(&s.numAccessors, 1)
	s.wgAccess.Add(1)
}

func (s *baseSegment) DecAccessor() {
	atomic.AddInt32(&s.numAccessors, -1)
	s.wgAccess.Done()
}

func (s *baseSegment) NumAccessors() int { return int(atomic.LoadInt32(&s.numAccessors)) }
func (s *baseSegment) Close()            { s.wgAccess.Wait() }

// Mutable segment APIs.
func (s *baseSegment) SetNumDocuments(v int32) { s.numDocs = v }
func (s *baseSegment) SetMinTimeNanos(v int64) { s.minTimeNanos = v }
func (s *baseSegment) SetMaxTimeNanos(v int64) { s.maxTimeNanos = v }
