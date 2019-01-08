package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/refcnt"
)

// TimeField contains data in documents for which such field are time values.
// TODO(xichen): Potentially support query APIs.
type TimeField interface {
	// DocIDSet returns the doc ID set for which the documents have time values.
	DocIDSet() DocIDSet

	// Values return the collection of time values. The values collection remains
	// valid until the field is closed.
	Values() values.TimeValues
}

// CloseableTimeField is a time field that can be closed.
type CloseableTimeField interface {
	TimeField

	// ShallowCopy returns a shallow copy of the field sharing access to the
	// underlying resources. As such the resources held will not be released until
	// there are no more references to the field.
	ShallowCopy() CloseableTimeField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// timeFieldBuilder incrementally builds a time field.
type timeFieldBuilder interface {
	// Add adds a time value alongside its document ID.
	Add(docID int32, v int64) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() CloseableTimeField

	// Seal seals and closes the time builder and returns an immutable time field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) CloseableTimeField

	// Close closes the builder.
	Close()
}

var (
	errTimeFieldBuilderAlreadyClosed = errors.New("time field builder is already closed")
)

type timeField struct {
	*refcnt.RefCounter

	docIDSet DocIDSet
	values   values.CloseableTimeValues
	closeFn  FieldCloseFn

	closed bool
}

// NewCloseableTimeField creates a time field.
func NewCloseableTimeField(
	docIDSet DocIDSet,
	values values.CloseableTimeValues,
) CloseableTimeField {
	return NewCloseableTimeFieldWithCloseFn(docIDSet, values, nil)
}

// NewCloseableTimeFieldWithCloseFn creates a time field with a close function.
func NewCloseableTimeFieldWithCloseFn(
	docIDSet DocIDSet,
	values values.CloseableTimeValues,
	closeFn FieldCloseFn,
) CloseableTimeField {
	return &timeField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		values:     values,
		closeFn:    closeFn,
	}
}

func (f *timeField) DocIDSet() DocIDSet        { return f.docIDSet }
func (f *timeField) Values() values.TimeValues { return f.values }

func (f *timeField) ShallowCopy() CloseableTimeField {
	f.IncRef()
	shallowCopy := *f
	return &shallowCopy
}

func (f *timeField) Close() {
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

type builderOfTimeField struct {
	dsb docIDSetBuilder
	svb timeValuesBuilder

	closed bool
}

func newTimeFieldBuilder(
	dsb docIDSetBuilder,
	svb timeValuesBuilder,
) *builderOfTimeField {
	return &builderOfTimeField{dsb: dsb, svb: svb}
}

func (b *builderOfTimeField) Add(docID int32, v int64) error {
	if b.closed {
		return errTimeFieldBuilderAlreadyClosed
	}
	b.dsb.Add(docID)
	return b.svb.Add(v)
}

func (b *builderOfTimeField) Snapshot() CloseableTimeField {
	docIDSetSnapshot := b.dsb.Snapshot()
	timeValuesSnapshot := b.svb.Snapshot()
	return NewCloseableTimeField(docIDSetSnapshot, timeValuesSnapshot)
}

func (b *builderOfTimeField) Seal(numTotalDocs int32) CloseableTimeField {
	docIDSet := b.dsb.Seal(numTotalDocs)
	values := b.svb.Seal()
	sealed := NewCloseableTimeField(docIDSet, values)

	// Clear and close the builder so it's no longer writable.
	*b = builderOfTimeField{}
	b.Close()

	return sealed
}

func (b *builderOfTimeField) Close() {
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
