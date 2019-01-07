package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
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
	docIDSet DocIDSet
	values   values.CloseableBoolValues
}

func (sf *boolField) DocIDSet() DocIDSet        { return sf.docIDSet }
func (sf *boolField) Values() values.BoolValues { return sf.values }
func (sf *boolField) Close()                    { sf.values.Close() }

type builderOfBoolField struct {
	dsb docIDSetBuilder
	svb boolValuesBuilder

	closed bool
}

func newBoolFieldBuilder(
	dsb docIDSetBuilder,
	svb boolValuesBuilder,
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
	return &boolField{docIDSet: docIDSetSnapshot, values: boolValuesSnapshot}
}

func (b *builderOfBoolField) Seal(numTotalDocs int32) CloseableBoolField {
	sealed := &boolField{
		docIDSet: b.dsb.Seal(numTotalDocs),
		values:   b.svb.Seal(),
	}

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
