package field

import (
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/x/refcnt"
)

// NullField contains data in documents for which such field are null values.
type NullField interface {
	// DocIDSet returns the doc ID set for which the documents have null values.
	DocIDSet() index.DocIDSet

	// Iter returns the field iterator.
	Iter() NullFieldIterator

	// Filter applies the given filter against the field, returning a doc
	// ID set iterator that returns the documents matching the filter.
	Filter(
		op filter.Op,
		filterValue *field.ValueUnion,
		numTotalDocs int32,
	) (index.DocIDSetIterator, error)

	// Fetch fetches the field doc IDs from the set of documents given by
	// the doc ID set iterator passed in. If the field doesn't exist in
	// a document from the doc ID set iterator output, it is ignored.
	Fetch(it index.DocIDSetIterator) NullFieldIterator
}

// CloseableNullField is a null field that can be closed.
type CloseableNullField interface {
	NullField

	// ShallowCopy returns a shallow copy of the field sharing access to the
	// underlying resources. As such the resources held will not be released until
	// there are no more references to the field.
	ShallowCopy() CloseableNullField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// nullFieldBuilder incrementally builds a null field.
type nullFieldBuilder interface {
	// Add adds a document ID for a null value.
	Add(docID int32) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() CloseableNullField

	// Seal seals and closes the null builder and returns an immutable null field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) CloseableNullField

	// Close closes the builder.
	Close()
}

var (
	errNullFieldBuilderAlreadyClosed = errors.New("null field builder is already closed")
)

type nullField struct {
	*refcnt.RefCounter

	docIDSet index.DocIDSet
	closeFn  CloseFn

	closed bool
}

// NewCloseableNullField creates a null field.
func NewCloseableNullField(
	docIDSet index.DocIDSet,
) CloseableNullField {
	return NewCloseableNullFieldWithCloseFn(docIDSet, nil)
}

// NewCloseableNullFieldWithCloseFn creates a int field with a close function.
func NewCloseableNullFieldWithCloseFn(
	docIDSet index.DocIDSet,
	closeFn CloseFn,
) CloseableNullField {
	return &nullField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		closeFn:    closeFn,
	}
}

func (f *nullField) DocIDSet() index.DocIDSet { return f.docIDSet }

func (f *nullField) Iter() NullFieldIterator { return newNullFieldIterator(f.docIDSet.Iter()) }

func (f *nullField) Filter(
	op filter.Op,
	_ *field.ValueUnion,
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
	return docIDSetIter, nil
}

func (f *nullField) Fetch(it index.DocIDSetIterator) NullFieldIterator {
	docIDPosIt := f.docIDSet.Intersect(it)
	return newNullFieldIterator(docIDPosIt)
}

func (f *nullField) ShallowCopy() CloseableNullField {
	f.IncRef()
	shallowCopy := *f
	return &shallowCopy
}

func (f *nullField) Close() {
	if f.closed {
		return
	}
	f.closed = true
	if f.DecRef() > 0 {
		return
	}
	f.docIDSet = nil
	if f.closeFn != nil {
		f.closeFn()
		f.closeFn = nil
	}
}

type builderOfNullField struct {
	dsb index.DocIDSetBuilder

	closed bool
}

func newNullFieldBuilder(
	dsb index.DocIDSetBuilder,
) *builderOfNullField {
	return &builderOfNullField{dsb: dsb}
}

func (b *builderOfNullField) Add(docID int32) error {
	if b.closed {
		return errNullFieldBuilderAlreadyClosed
	}
	b.dsb.Add(docID)
	return nil
}

func (b *builderOfNullField) Snapshot() CloseableNullField {
	docIDSetSnapshot := b.dsb.Snapshot()
	return NewCloseableNullField(docIDSetSnapshot)
}

func (b *builderOfNullField) Seal(numTotalDocs int32) CloseableNullField {
	docIDSet := b.dsb.Seal(numTotalDocs)
	sealed := NewCloseableNullField(docIDSet)

	// Clear and close the builder so it's no longer writable.
	*b = builderOfNullField{}
	b.Close()

	return sealed
}

func (b *builderOfNullField) Close() {
	if b.closed {
		return
	}
	b.closed = true
	b.dsb = nil
}
