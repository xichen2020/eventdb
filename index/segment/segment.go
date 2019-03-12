package segment

import (
	"sync"

	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/hash"
)

// FieldFn applies a function against a segment field.
type FieldFn func(f *Field)

// Field contains a field along with its type and value metadata.
type Field struct {
	// Immutable internal fields.
	path       []string
	types      []field.ValueType
	valueMetas []values.MetaUnion

	// Mutable internal fields protected by a lock.
	fieldLock sync.RWMutex
	field     indexfield.DocsField
}

type ownershipMode int

const (
	transferOwnership ownershipMode = iota
	sharedOwnership
)

// nolint: unparam
func newField(
	df indexfield.DocsField,
	ownershipMode ownershipMode,
) *Field {
	if ownershipMode == sharedOwnership {
		df = df.ShallowCopy()
	}
	fm := df.Metadata()
	valueMetas := make([]values.MetaUnion, 0, len(fm.FieldTypes))
	for _, t := range fm.FieldTypes {
		tf, _ := df.FieldForType(t)
		valueMetas = append(valueMetas, tf.MustValuesMeta())
	}
	return &Field{
		path:       fm.FieldPath,
		types:      fm.FieldTypes,
		valueMetas: valueMetas,
		field:      df,
	}
}

// Types contain the list of field types.
func (f *Field) Types() []field.ValueType { return f.types }

// ValueMetas contains the list of value metas.
func (f *Field) ValueMetas() []values.MetaUnion { return f.valueMetas }

// DocsField returns the raw document field.
// If the raw docs field is not nil, its refcount is incremented, and the returned
// docs field should be closed after use.
// Otherwise, nil is returned.
func (f *Field) DocsField() indexfield.DocsField {
	f.fieldLock.RLock()
	defer f.fieldLock.RUnlock()

	if f.field == nil {
		return nil
	}
	return f.field.ShallowCopy()
}

// Insert inserts the incoming docs field into the segment field object.
// The incoming docs field remains valid after the merge and should be closed where appropriate.
func (f *Field) Insert(df indexfield.DocsField) {
	f.fieldLock.Lock()
	defer f.fieldLock.Unlock()

	if f.field == nil {
		f.field = df.ShallowCopy()
		return
	}
	merged := f.field.NewMergedDocsField(df)
	f.field.Close()
	f.field = merged
}

// clear closes and nils out the field values to reduce memory usage.
// but keeps the metadata to facilitate reloading the fields.
func (f *Field) clear() {
	f.fieldLock.Lock()
	defer f.fieldLock.Unlock()

	if f.field == nil {
		return
	}
	f.field.Close()
	f.field = nil
}

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
// The segment is not thread-safe. Concurrently access to the segment (e.g., reading
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
