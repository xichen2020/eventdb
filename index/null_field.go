package index

import "errors"

// NullField contains data in documents for which such field are null values.
// TODO(xichen): Potentially support query APIs.
type NullField interface {
	// DocIDSet returns the doc ID set for which the documents have null values.
	DocIDSet() DocIDSet
}

type closeableNullField interface {
	NullField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// nullFieldBuilder incrementally builds a null field.
type nullFieldBuilder interface {
	// Add adds a document ID for a null value.
	Add(docID int32) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() closeableNullField

	// Seal seals and closes the null builder and returns an immutable null field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) closeableNullField

	// Close closes the builder.
	Close()
}

var (
	errNullFieldBuilderAlreadyClosed = errors.New("null field builder is already closed")
)

type nullField struct {
	docIDSet DocIDSet
}

func (sf *nullField) DocIDSet() DocIDSet { return sf.docIDSet }
func (sf *nullField) Close()             {}

type builderOfNullField struct {
	dsb docIDSetBuilder

	closed bool
}

func newNullFieldBuilder(
	dsb docIDSetBuilder,
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

func (b *builderOfNullField) Snapshot() closeableNullField {
	docIDSetSnapshot := b.dsb.Snapshot()
	return &nullField{docIDSet: docIDSetSnapshot}
}

func (b *builderOfNullField) Seal(numTotalDocs int32) closeableNullField {
	sealed := &nullField{
		docIDSet: b.dsb.Seal(numTotalDocs),
	}

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
