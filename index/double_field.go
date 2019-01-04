package index

import "errors"

// DoubleField contains data in documents for which such field are double values.
// TODO(xichen): Potentially support query APIs.
type DoubleField interface {
	// DocIDSet returns the doc ID set for which the documents have double values.
	DocIDSet() DocIDSet

	// Values return the collection of double values. The values collection remains
	// valid until the field is closed.
	Values() DoubleValues
}

type closeableDoubleField interface {
	DoubleField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// doubleFieldBuilder incrementally builds a double field.
type doubleFieldBuilder interface {
	// Add adds a double value alongside its document ID.
	Add(docID int32, v float64) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() closeableDoubleField

	// Seal seals and closes the double builder and returns an immutable double field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) closeableDoubleField

	// Close closes the builder.
	Close()
}

var (
	errDoubleFieldBuilderAlreadyClosed = errors.New("double field builder is already closed")
)

type doubleField struct {
	docIDSet DocIDSet
	values   closeableDoubleValues
}

func (sf *doubleField) DocIDSet() DocIDSet   { return sf.docIDSet }
func (sf *doubleField) Values() DoubleValues { return sf.values }
func (sf *doubleField) Close()               { sf.values.Close() }

type builderOfDoubleField struct {
	dsb docIDSetBuilder
	svb doubleValuesBuilder

	closed bool
}

func newDoubleFieldBuilder(
	dsb docIDSetBuilder,
	svb doubleValuesBuilder,
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

func (b *builderOfDoubleField) Snapshot() closeableDoubleField {
	docIDSetSnapshot := b.dsb.Snapshot()
	doubleValuesSnapshot := b.svb.Snapshot()
	return &doubleField{docIDSet: docIDSetSnapshot, values: doubleValuesSnapshot}
}

func (b *builderOfDoubleField) Seal(numTotalDocs int32) closeableDoubleField {
	sealed := &doubleField{
		docIDSet: b.dsb.Seal(numTotalDocs),
		values:   b.svb.Seal(),
	}

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
