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

// DoubleField contains data in documents for which such field are double values.
type DoubleField interface {
	// DocIDSet returns the doc ID set for which the documents have double values.
	DocIDSet() index.DocIDSet

	// Values return the collection of double values. The values collection remains
	// valid until the field is closed.
	Values() values.DoubleValues

	// Iter returns the field iterator.
	Iter() (DoubleFieldIterator, error)

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
	Fetch(it index.DocIDSetIterator) (DoubleFieldIterator, error)
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

	docIDSet index.DocIDSet
	values   values.CloseableDoubleValues
	closeFn  CloseFn

	closed bool
}

// NewCloseableDoubleField creates a double field.
func NewCloseableDoubleField(
	docIDSet index.DocIDSet,
	values values.CloseableDoubleValues,
) CloseableDoubleField {
	return NewCloseableDoubleFieldWithCloseFn(docIDSet, values, nil)
}

// NewCloseableDoubleFieldWithCloseFn creates a double field with a close function.
func NewCloseableDoubleFieldWithCloseFn(
	docIDSet index.DocIDSet,
	values values.CloseableDoubleValues,
	closeFn CloseFn,
) CloseableDoubleField {
	return &doubleField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		values:     values,
		closeFn:    closeFn,
	}
}

func (f *doubleField) DocIDSet() index.DocIDSet    { return f.docIDSet }
func (f *doubleField) Values() values.DoubleValues { return f.values }

func (f *doubleField) Iter() (DoubleFieldIterator, error) {
	valsIt, err := f.values.Iter()
	if err != nil {
		return nil, err
	}
	return newDoubleFieldIterator(f.docIDSet.Iter(), valsIt), nil
}

func (f *doubleField) Filter(
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

func (f *doubleField) Fetch(it index.DocIDSetIterator) (DoubleFieldIterator, error) {
	valsIt, err := f.values.Iter()
	if err != nil {
		return nil, err
	}
	docIDPosIt := f.docIDSet.Intersect(it)
	return newAtPositionDoubleFieldIterator(docIDPosIt, valsIt), nil
}

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
	dsb index.DocIDSetBuilder
	svb values.DoubleValuesBuilder

	closed bool
}

func newDoubleFieldBuilder(
	dsb index.DocIDSetBuilder,
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
