package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/refcnt"
)

// DoubleField contains data in documents for which such field are double values.
// TODO(xichen): Potentially support query APIs.
type DoubleField interface {
	// DocIDSet returns the doc ID set for which the documents have double values.
	DocIDSet() DocIDSet

	// Values return the collection of double values. The values collection remains
	// valid until the field is closed.
	Values() values.DoubleValues
}

// CloseableDoubleField is a double field that can be closed.
type CloseableDoubleField interface {
	DoubleField

	// ShallowCopy returns a shallow copy of the field sharing access to the
	// underlying resources. As such the resources held will not be released until
	// there are no more references to the field.
	ShallowCopy() CloseableDoubleField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// doubleFieldBuilder incrementally builds a double field.
type doubleFieldBuilder interface {
	// Add adds a double value alongside its document ID.
	Add(docID int32, v float64) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() CloseableDoubleField

	// Seal seals and closes the double builder and returns an immutable double field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) CloseableDoubleField

	// Close closes the builder.
	Close()
}

var (
	errDoubleFieldBuilderAlreadyClosed = errors.New("double field builder is already closed")
)

type doubleField struct {
	*refcnt.RefCounter

	docIDSet DocIDSet
	values   values.CloseableDoubleValues
	closeFn  FieldCloseFn

	closed bool
}

// NewCloseableDoubleField creates a double field.
func NewCloseableDoubleField(
	docIDSet DocIDSet,
	values values.CloseableDoubleValues,
) CloseableDoubleField {
	return NewCloseableDoubleFieldWithCloseFn(docIDSet, values, nil)
}

// NewCloseableDoubleFieldWithCloseFn creates a double field with a close function.
func NewCloseableDoubleFieldWithCloseFn(
	docIDSet DocIDSet,
	values values.CloseableDoubleValues,
	closeFn FieldCloseFn,
) CloseableDoubleField {
	return &doubleField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		values:     values,
		closeFn:    closeFn,
	}
}

func (f *doubleField) DocIDSet() DocIDSet          { return f.docIDSet }
func (f *doubleField) Values() values.DoubleValues { return f.values }

func (f *doubleField) ShallowCopy() CloseableDoubleField {
	f.IncRef()
	shallowCopy := *f
	return &shallowCopy
}

func (f *doubleField) Close() {
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

type builderOfDoubleField struct {
	dsb docIDSetBuilder
	svb values.DoubleValuesBuilder

	closed bool
}

func newDoubleFieldBuilder(
	dsb docIDSetBuilder,
	svb values.DoubleValuesBuilder,
) *builderOfDoubleField {
	return &builderOfDoubleField{dsb: dsb, svb: svb}
}

func (b *builderOfDoubleField) Add(docID int32, v float64) error {
	if b.closed {
		return errDoubleFieldBuilderAlreadyClosed
	}
	b.dsb.Add(docID)
	return b.svb.Add(v)
}

func (b *builderOfDoubleField) Snapshot() CloseableDoubleField {
	docIDSetSnapshot := b.dsb.Snapshot()
	doubleValuesSnapshot := b.svb.Snapshot()
	return NewCloseableDoubleField(docIDSetSnapshot, doubleValuesSnapshot)
}

func (b *builderOfDoubleField) Seal(numTotalDocs int32) CloseableDoubleField {
	docIDSet := b.dsb.Seal(numTotalDocs)
	values := b.svb.Seal()
	sealed := NewCloseableDoubleField(docIDSet, values)

	// Clear and close the builder so it's no longer writable.
	*b = builderOfDoubleField{}
	b.Close()

	return sealed
}

func (b *builderOfDoubleField) Close() {
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
