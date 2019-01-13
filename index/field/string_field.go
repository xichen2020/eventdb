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

// StringField contains data in documents for which such field are string values.
type StringField interface {
	// DocIDSet returns the doc ID set for which the documents have string values.
	DocIDSet() index.DocIDSet

	// Values return the collection of string values. The values collection remains
	// valid until the field is closed.
	Values() values.StringValues

	// Iter returns the field iterator.
	Iter() (StringFieldIterator, error)

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
	Fetch(it index.DocIDSetIterator) (StringFieldIterator, error)
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

	docIDSet index.DocIDSet
	values   values.CloseableStringValues
	closeFn  CloseFn

	closed bool
}

// NewCloseableStringField creates a string field.
func NewCloseableStringField(
	docIDSet index.DocIDSet,
	values values.CloseableStringValues,
) CloseableStringField {
	return NewCloseableStringFieldWithCloseFn(docIDSet, values, nil)
}

// NewCloseableStringFieldWithCloseFn creates a string field with a close function.
func NewCloseableStringFieldWithCloseFn(
	docIDSet index.DocIDSet,
	values values.CloseableStringValues,
	closeFn CloseFn,
) CloseableStringField {
	return &stringField{
		RefCounter: refcnt.NewRefCounter(),
		docIDSet:   docIDSet,
		values:     values,
		closeFn:    closeFn,
	}
}

func (f *stringField) DocIDSet() index.DocIDSet    { return f.docIDSet }
func (f *stringField) Values() values.StringValues { return f.values }

func (f *stringField) Iter() (StringFieldIterator, error) {
	valsIt, err := f.values.Iter()
	if err != nil {
		return nil, err
	}
	return newStringFieldIterator(f.docIDSet.Iter(), valsIt, field.NewStringUnion), nil
}

func (f *stringField) Filter(
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

func (f *stringField) Fetch(it index.DocIDSetIterator) (StringFieldIterator, error) {
	valsIt, err := f.values.Iter()
	if err != nil {
		return nil, err
	}
	docIDPosIt := f.docIDSet.Intersect(it)
	return newAtPositionStringFieldIterator(docIDPosIt, valsIt, field.NewStringUnion), nil
}

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
	dsb index.DocIDSetBuilder
	svb values.StringValuesBuilder

	closed bool
}

func newStringFieldBuilder(
	dsb index.DocIDSetBuilder,
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
