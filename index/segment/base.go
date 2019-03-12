package segment

import (
	"github.com/xichen2020/eventdb/x/refcnt"
)

// base is a base segment.
type base interface {
	// Metadata returns the segment metadata.
	Metadata() Metadata

	// Intersects returns true if the time range associated with the documents in
	// the segment intersects with the query time range. If the segment is empty,
	// this returns false.
	Intersects(startNanosInclusive, endNanosExclusive int64) bool
}

// refCountedBase is a ref-counted segment.
type refCountedBase interface {
	base

	// RefCount	returns the reference count of the segment.
	RefCount() int32

	// IncRef increments the refcount.
	IncRef()

	// SetCloser sets the callback to be called when the segment is closed.
	SetCloser(fn func())

	// Close decrements the refcount and closes the segment if the refcnt drops to zero.
	Close()
}

// baseSegment contains the base segment metadata.
// NB: baseSegment is not thread-safe. Concurrent access to base segment must be
// protected externally.
type baseSegment struct {
	m Metadata
}

func newBaseSegment(meta Metadata) *baseSegment {
	return &baseSegment{m: meta}
}

func (s *baseSegment) Metadata() Metadata { return s.m }

func (s *baseSegment) Intersects(startNanosInclusive, endNanosExclusive int64) bool {
	if s.m.NumDocs == 0 {
		return false
	}
	return s.m.MinTimeNanos < endNanosExclusive && s.m.MaxTimeNanos >= startNanosInclusive
}

type refCountedBaseSegment struct {
	*baseSegment

	rc     *refcnt.RefCounter
	closer func()
}

func newRefCountedBaseSegment(
	base *baseSegment,
	closer func(),
) *refCountedBaseSegment {
	return &refCountedBaseSegment{
		baseSegment: base,
		rc:          refcnt.NewRefCounter(),
		closer:      closer,
	}
}

func (s *refCountedBaseSegment) RefCount() int32     { return s.rc.RefCount() }
func (s *refCountedBaseSegment) IncRef()             { s.rc.IncRef() }
func (s *refCountedBaseSegment) SetCloser(fn func()) { s.closer = fn }

func (s *refCountedBaseSegment) Close() {
	if s.rc.DecRef() > 0 {
		return
	}
	if s.closer != nil {
		s.closer()
	}
}
