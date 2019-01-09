package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/refcnt"
)

// BoolField contains data in documents for which such field are bool values.
// TODO(xichen): Potentially support query APIs.
type BoolField interface {
	// DocIDSet returns the doc ID set for which the documents have bool values.
	DocIDSet() DocIDSet

	// Values return the collection of bool values. The values collection remains
	// valid until the field is closed.
	Values() values.BoolValues
}

// CloseableBoolField is a bool field that can be closed.
type CloseableBoolField interface {
	BoolField

	// ShallowCopy returns a shallow copy of the field sharing access to the
	// underlying resources. As such the resources held will not be released until
	// there are no more references to the field.
	ShallowCopy() CloseableBoolField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// boolFieldBuilder incrementally builds a bool field.
type boolFieldBuilder interface {
	// Add adds a bool value alongside its document ID.
	Add(docID int32, v bool) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() CloseableBoolField

	// Seal seals and closes the bool builder and returns an immutable bool field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) CloseableBoolField

	// Close closes the builder.
	Close()
}

var (
	errBoolFieldBuilderAlreadyClosed = errors.New("bool field builder is already closed")
)

type boolField struct {
	*refcnt.RefCounter

	docIDSet DocIDSet
	values   values.CloseableBoolValues
	closeFn  FieldCloseFn

	closed bool
}

// NewCloseableBoolField creates a bool field.
func NewCloseableBoolField(
	docIDSet DocIDSet,
	values values.CloseableBoolValues,
) CloseableBoolField {
	return NewCloseableBoolFieldWithCloseFn(docIDSet, values, nil)
}

// NewCloseableBoolFieldWithCloseFn creates a bool field with a close function.
func NewCloseableBoolFieldWithCloseFn(
	docIDSet DocIDSet,
	values values.CloseableBoolValues,
	closeFn FieldCloseFn,
) CloseableBoolField {
	return &boolField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		values:     values,
		closeFn:    closeFn,
	}
}

func (f *boolField) DocIDSet() DocIDSet        { return f.docIDSet }
func (f *boolField) Values() values.BoolValues { return f.values }

func (f *boolField) ShallowCopy() CloseableBoolField {
	f.IncRef()
	shallowCopy := *f
	return &shallowCopy
}

func (f *boolField) Close() {
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

type builderOfBoolField struct {
	dsb docIDSetBuilder
	svb values.BoolValuesBuilder

	closed bool
}

func newBoolFieldBuilder(
	dsb docIDSetBuilder,
	svb values.BoolValuesBuilder,
) *builderOfBoolField {
	return &builderOfBoolField{dsb: dsb, svb: svb}
}

func (b *builderOfBoolField) Add(docID int32, v bool) error {
	if b.closed {
		return errBoolFieldBuilderAlreadyClosed
	}
	b.dsb.Add(docID)
	return b.svb.Add(v)
}

func (b *builderOfBoolField) Snapshot() CloseableBoolField {
	docIDSetSnapshot := b.dsb.Snapshot()
	boolValuesSnapshot := b.svb.Snapshot()
	return NewCloseableBoolField(docIDSetSnapshot, boolValuesSnapshot)
}

func (b *builderOfBoolField) Seal(numTotalDocs int32) CloseableBoolField {
	docIDSet := b.dsb.Seal(numTotalDocs)
	values := b.svb.Seal()
	sealed := NewCloseableBoolField(docIDSet, values)

	// Clear and close the builder so it's no longer writable.
	*b = builderOfBoolField{}
	b.Close()

	return sealed
}

func (b *builderOfBoolField) Close() {
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
