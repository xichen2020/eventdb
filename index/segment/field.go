package segment

import (
	"sync"

	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/values"
)

// FieldFn applies a function against a segment field.
type FieldFn func(f *Field)

// Field contains a field along with its types and value metadatas.
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

// clear closes and nils out the field values to reduce memory usage,
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
