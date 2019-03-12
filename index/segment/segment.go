package segment

import (
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/x/hash"
)

// ImmutableSegment is an immutable segment.
type ImmutableSegment interface {
	refCountedBase

	// FieldAt returns the field at a given path.
	FieldAt(fieldPath []string) (*Field, bool)

	// ForEach performs an operation against each segment field.
	ForEach(fn FieldFn)
}

// MutableSegment is a mutable segment.
type MutableSegment interface {
	ImmutableSegment

	// ClearAll clears all fields in the segment.
	ClearAll()
}

// segmentOptions provide a set of segment options.
type segmentOptions struct {
	timestampFieldPath    []string
	rawDocSourceFieldPath []string
	fieldHashFn           hash.StringArrayHashFn
}

// segment is a refcounted index segment.
// The data in the segment fields may change, but the field metadata in the field map
// does not change until the segment is closed, in which case the map is nil'ed out.
// The segment is not thread-safe. Concurrent access to the segment (e.g., reading
// a segment, clearing a segment, closing a segment, etc.) must be protected with
// synchronization primitives.
type segment struct {
	refCountedBase

	timestampFieldPath    []string
	rawDocSourceFieldPath []string
	fieldHashFn           hash.StringArrayHashFn

	closed bool
	fields map[hash.Hash]*Field
}

// newSegmentWithBuilder creates a new segment with field builders.
func newSegmentWithBuilder(
	meta Metadata,
	fieldBuilders map[hash.Hash]indexfield.DocsFieldBuilder,
	opts *segmentOptions,
) *segment {
	fields := make(map[hash.Hash]*Field, len(fieldBuilders))
	for k, f := range fieldBuilders {
		sealed := f.Seal(meta.NumDocs)
		fields[k] = newField(sealed, transferOwnership)
	}

	s := &segment{
		refCountedBase:        newRefCountedBaseSegment(newBaseSegment(meta), nil),
		timestampFieldPath:    opts.timestampFieldPath,
		rawDocSourceFieldPath: opts.rawDocSourceFieldPath,
		fieldHashFn:           opts.fieldHashFn,
		fields:                fields,
	}

	// Make sure we clean up the segment after the refcount drops to zero.
	s.SetCloser(s.close)
	return s
}

func (s *segment) FieldAt(fieldPath []string) (*Field, bool) {
	fieldHash := s.fieldHashFn(fieldPath)
	field, exists := s.fields[fieldHash]
	return field, exists
}

func (s *segment) ForEach(fn FieldFn) {
	for _, field := range s.fields {
		fn(field)
	}
}

func (s *segment) ClearAll() {
	for _, field := range s.fields {
		field.clear()
	}
}

func (s *segment) close() {
	if s.closed {
		return
	}
	s.closed = true
	s.timestampFieldPath = nil
	s.rawDocSourceFieldPath = nil
	s.fieldHashFn = nil
	s.ClearAll()
	s.fields = nil
}
