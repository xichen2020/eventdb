package index

import "errors"

// StringField contains data in documents for which such field are string values.
// TODO(xichen): Potentially support query APIs.
type StringField interface {
	// DocIDSet returns the doc ID set for which the documents have string values.
	DocIDSet() DocIDSet

	// Values return the collection of string values. The values collection remains
	// valid until the field is closed.
	Values() StringValues
}

type closeableStringField interface {
	StringField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// stringFieldBuilder incrementally builds a string field.
type stringFieldBuilder interface {
	// Add adds a string value alongside its document ID.
	Add(docID int32, v string) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() closeableStringField

	// Seal seals and closes the string builder and returns an immutable string field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) closeableStringField

	// Close closes the builder.
	Close()
}

var (
	errStringFieldBuilderAlreadyClosed = errors.New("string field builder is already closed")
)

type stringField struct {
	docIDSet DocIDSet
	values   closeableStringValues
}

func (sf *stringField) DocIDSet() DocIDSet   { return sf.docIDSet }
func (sf *stringField) Values() StringValues { return sf.values }
func (sf *stringField) Close()               { sf.values.Close() }

type builderOfStringField struct {
	dsb docIDSetBuilder
	svb stringValuesBuilder

	closed bool
}

func newStringFieldBuilder(
	dsb docIDSetBuilder,
	svb stringValuesBuilder,
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

func (b *builderOfStringField) Snapshot() closeableStringField {
	docIDSetSnapshot := b.dsb.Snapshot()
	stringValuesSnapshot := b.svb.Snapshot()
	return &stringField{docIDSet: docIDSetSnapshot, values: stringValuesSnapshot}
}

func (b *builderOfStringField) Seal(numTotalDocs int32) closeableStringField {
	sealed := &stringField{
		docIDSet: b.dsb.Seal(numTotalDocs),
		values:   b.svb.Seal(),
	}

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