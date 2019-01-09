package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/refcnt"
)

// StringField contains data in documents for which such field are string values.
// TODO(xichen): Potentially support query APIs.
type StringField interface {
	// DocIDSet returns the doc ID set for which the documents have string values.
	DocIDSet() DocIDSet

	// Values return the collection of string values. The values collection remains
	// valid until the field is closed.
	Values() values.StringValues
}

// CloseableStringField is a string field that can be closed.
type CloseableStringField interface {
	StringField

	// ShallowCopy returns a shallow copy of the field sharing access to the
	// underlying resources. As such the resources held will not be released until
	// there are no more references to the field.
	ShallowCopy() CloseableStringField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// stringFieldBuilder incrementally builds a string field.
type stringFieldBuilder interface {
	// Add adds a string value alongside its document ID.
	Add(docID int32, v string) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() CloseableStringField

	// Seal seals and closes the string builder and returns an immutable string field.
	// The resource ownership is tranferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) CloseableStringField

	// Close closes the builder.
	Close()
}

var (
	errStringFieldBuilderAlreadyClosed = errors.New("string field builder is already closed")
)

type stringField struct {
	*refcnt.RefCounter

	docIDSet DocIDSet
	values   values.CloseableStringValues
	closeFn  FieldCloseFn

	closed bool
}

// NewCloseableStringField creates a string field.
func NewCloseableStringField(
	docIDSet DocIDSet,
	values values.CloseableStringValues,
) CloseableStringField {
	return NewCloseableStringFieldWithCloseFn(docIDSet, values, nil)
}

// NewCloseableStringFieldWithCloseFn creates a string field with a close function.
func NewCloseableStringFieldWithCloseFn(
	docIDSet DocIDSet,
	values values.CloseableStringValues,
	closeFn FieldCloseFn,
) CloseableStringField {
	return &stringField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		values:     values,
		closeFn:    closeFn,
	}
}

func (f *stringField) DocIDSet() DocIDSet          { return f.docIDSet }
func (f *stringField) Values() values.StringValues { return f.values }

func (f *stringField) ShallowCopy() CloseableStringField {
	f.IncRef()
	shallowCopy := *f
	return &shallowCopy
}

func (f *stringField) Close() {
	if f.closed {
		return
	}
	f.closed = true
	if f.DecRef() > 0 {
		return
	}
	f.docIDSet = nil
	f.values.Close()
	f.values = nil
	if f.closeFn != nil {
		f.closeFn()
		f.closeFn = nil
	}
}

type builderOfStringField struct {
	dsb docIDSetBuilder
	svb values.StringValuesBuilder

	closed bool
}

func newStringFieldBuilder(
	dsb docIDSetBuilder,
	svb values.StringValuesBuilder,
) *builderOfStringField {
	return &builderOfStringField{dsb: dsb, svb: svb}
}

func (b *builderOfStringField) Add(docID int32, v string) error {
	if b.closed {
		return errStringFieldBuilderAlreadyClosed
	}
	b.dsb.Add(docID)
	return b.svb.Add(v)
}

func (b *builderOfStringField) Snapshot() CloseableStringField {
	docIDSetSnapshot := b.dsb.Snapshot()
	stringValuesSnapshot := b.svb.Snapshot()
	return NewCloseableStringField(docIDSetSnapshot, stringValuesSnapshot)
}

func (b *builderOfStringField) Seal(numTotalDocs int32) CloseableStringField {
	docIDSet := b.dsb.Seal(numTotalDocs)
	values := b.svb.Seal()
	sealed := NewCloseableStringField(docIDSet, values)

	// Clear and close the builder so it's no longer writable.
	*b = builderOfStringField{}
	b.Close()

	return sealed
}

func (b *builderOfStringField) Close() {
	if b.closed {
		return
	}
	b.closed = true
	b.dsb = nil
	if b.svb != nil {
		b.svb.Close()
		b.svb = nil
	}
}
