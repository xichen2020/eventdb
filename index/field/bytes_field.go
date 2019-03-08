package field

import (
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/refcnt"
)

// BytesField contains data in documents for which such field are bytes values.
type BytesField interface {
	// DocIDSet returns the doc ID set for which the documents have bytes values.
	DocIDSet() index.DocIDSet

	// Values return the collection of bytes values. The values collection remains
	// valid until the field is closed.
	Values() values.BytesValues

	// Iter returns the field iterator.
	Iter() (BytesFieldIterator, error)

	// Filter applies the given filter against the field, returning a doc
	// ID set iterator that returns the documents matching the filter.
	Filter(
		op filter.Op,
		filterValue *field.ValueUnion,
		numTotalDocs int32,
	) (index.DocIDSetIterator, error)

	// Fetch fetches the field values from the set of documents given by
	// the doc ID set iterator passed in. If the field doesn't exist in
	// a document from the doc ID set iterator output, it is ignored.
	Fetch(it index.DocIDSetIterator) (MaskingBytesFieldIterator, error)
}

// CloseableBytesField is a bytes field that can be closed.
type CloseableBytesField interface {
	BytesField

	// ShallowCopy returns a shallow copy of the field sharing access to the
	// underlying resources. As such the resources held will not be released until
	// there are no more references to the field.
	ShallowCopy() CloseableBytesField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// bytesFieldBuilder incrementally builds a bytes field.
type bytesFieldBuilder interface {
	// Add adds a bytes value alongside its document ID.
	Add(docID int32, v []byte) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() CloseableBytesField

	// Seal seals and closes the bytes builder and returns an immutable bytes field.
	// The resource ownership is tranferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) CloseableBytesField

	// Close closes the builder.
	Close()
}

var (
	errBytesFieldBuilderAlreadyClosed = errors.New("bytes field builder is already closed")
)

type bytesField struct {
	*refcnt.RefCounter

	docIDSet index.DocIDSet
	values   values.CloseableBytesValues
	closeFn  CloseFn

	closed bool
}

// NewCloseableBytesField creates a bytes field.
func NewCloseableBytesField(
	docIDSet index.DocIDSet,
	values values.CloseableBytesValues,
) CloseableBytesField {
	return NewCloseableBytesFieldWithCloseFn(docIDSet, values, nil)
}

// NewCloseableBytesFieldWithCloseFn creates a bytes field with a close function.
func NewCloseableBytesFieldWithCloseFn(
	docIDSet index.DocIDSet,
	values values.CloseableBytesValues,
	closeFn CloseFn,
) CloseableBytesField {
	return &bytesField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		values:     values,
		closeFn:    closeFn,
	}
}

func (f *bytesField) DocIDSet() index.DocIDSet   { return f.docIDSet }
func (f *bytesField) Values() values.BytesValues { return f.values }

func (f *bytesField) Iter() (BytesFieldIterator, error) {
	valsIt, err := f.values.Iter()
	if err != nil {
		return nil, err
	}
	return newBytesFieldIterator(f.docIDSet.Iter(), valsIt, field.NewBytesUnion), nil
}

func (f *bytesField) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
	numTotalDocs int32,
) (index.DocIDSetIterator, error) {
	if !op.IsValid() {
		return nil, fmt.Errorf("invalid value filter: %v", op)
	}
	docIDSetIter := f.docIDSet.Iter()
	if op.IsDocIDSetFilter() {
		docIDSetIteratorFn := op.MustDocIDSetFilterFn(numTotalDocs)
		return docIDSetIteratorFn(docIDSetIter), nil
	}
	positionIt, err := f.values.Filter(op, filterValue)
	if err != nil {
		return nil, err
	}
	return index.NewAtPositionDocIDSetIterator(docIDSetIter, positionIt), nil
}

func (f *bytesField) Fetch(it index.DocIDSetIterator) (MaskingBytesFieldIterator, error) {
	valsIt, err := f.values.Iter()
	if err != nil {
		return nil, err
	}
	docIDPosIt := f.docIDSet.Intersect(it)
	return newAtPositionBytesFieldIterator(docIDPosIt, valsIt, field.NewBytesUnion), nil
}

func (f *bytesField) ShallowCopy() CloseableBytesField {
	f.IncRef()
	shallowCopy := *f
	return &shallowCopy
}

func (f *bytesField) Close() {
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

type builderOfBytesField struct {
	dsb index.DocIDSetBuilder
	svb values.BytesValuesBuilder

	closed bool
}

func newBytesFieldBuilder(
	dsb index.DocIDSetBuilder,
	svb values.BytesValuesBuilder,
) *builderOfBytesField {
	return &builderOfBytesField{dsb: dsb, svb: svb}
}

func (b *builderOfBytesField) Add(docID int32, v []byte) error {
	if b.closed {
		return errBytesFieldBuilderAlreadyClosed
	}
	b.dsb.Add(docID)
	return b.svb.Add(v)
}

func (b *builderOfBytesField) Snapshot() CloseableBytesField {
	docIDSetSnapshot := b.dsb.Snapshot()
	bytesValuesSnapshot := b.svb.Snapshot()
	return NewCloseableBytesField(docIDSetSnapshot, bytesValuesSnapshot)
}

func (b *builderOfBytesField) Seal(numTotalDocs int32) CloseableBytesField {
	docIDSet := b.dsb.Seal(numTotalDocs)
	values := b.svb.Seal()
	sealed := NewCloseableBytesField(docIDSet, values)

	// Clear and close the builder so it's no longer writable.
	*b = builderOfBytesField{}
	b.Close()

	return sealed
}

func (b *builderOfBytesField) Close() {
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
