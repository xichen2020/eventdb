package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
)

// IntField contains data in documents for which such field are int values.
// TODO(xichen): Potentially support query APIs.
type IntField interface {
	// DocIDSet returns the doc ID set for which the documents have int values.
	DocIDSet() DocIDSet

	// Values return the collection of int values. The values collection remains
	// valid until the field is closed.
	Values() values.IntValues
}

// CloseableIntField is an int field that can be closed.
type CloseableIntField interface {
	IntField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// intFieldBuilder incrementally builds a int field.
type intFieldBuilder interface {
	// Add adds a int value alongside its document ID.
	Add(docID int32, v int) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() CloseableIntField

	// Seal seals and closes the int builder and returns an immutable int field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) CloseableIntField

	// Close closes the builder.
	Close()
}

var (
	errIntFieldBuilderAlreadyClosed = errors.New("int field builder is already closed")
)

type intField struct {
	docIDSet DocIDSet
	values   values.CloseableIntValues
}

func (sf *intField) DocIDSet() DocIDSet       { return sf.docIDSet }
func (sf *intField) Values() values.IntValues { return sf.values }
func (sf *intField) Close()                   { sf.values.Close() }

type builderOfIntField struct {
	dsb docIDSetBuilder
	svb intValuesBuilder

	closed bool
}

func newIntFieldBuilder(
	dsb docIDSetBuilder,
	svb intValuesBuilder,
) *builderOfIntField {
	return &builderOfIntField{dsb: dsb, svb: svb}
}

func (b *builderOfIntField) Add(docID int32, v int) error {
	if b.closed {
		return errIntFieldBuilderAlreadyClosed
	}
	b.dsb.Add(docID)
	return b.svb.Add(v)
}

func (b *builderOfIntField) Snapshot() CloseableIntField {
	docIDSetSnapshot := b.dsb.Snapshot()
	intValuesSnapshot := b.svb.Snapshot()
	return &intField{docIDSet: docIDSetSnapshot, values: intValuesSnapshot}
}

func (b *builderOfIntField) Seal(numTotalDocs int32) CloseableIntField {
	sealed := &intField{
		docIDSet: b.dsb.Seal(numTotalDocs),
		values:   b.svb.Seal(),
	}

	// Clear and close the builder so it's no longer writable.
	*b = builderOfIntField{}
	b.Close()

	return sealed
}

func (b *builderOfIntField) Close() {
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
