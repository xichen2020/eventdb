package index

import "errors"

// TimeField contains data in documents for which such field are time values.
// TODO(xichen): Potentially support query APIs.
type TimeField interface {
	// DocIDSet returns the doc ID set for which the documents have time values.
	DocIDSet() DocIDSet

	// Values return the collection of time values. The values collection remains
	// valid until the field is closed.
	Values() TimeValues
}

type closeableTimeField interface {
	TimeField

	// Close closes the field to release the resources held for the collection.
	Close()
}

// timeFieldBuilder incrementally builds a time field.
type timeFieldBuilder interface {
	// Add adds a time value alongside its document ID.
	Add(docID int32, v int64) error

	// Snapshot take a snapshot of the field data accummulated so far.
	Snapshot() closeableTimeField

	// Seal seals and closes the time builder and returns an immutable time field.
	// The resource ownership is transferred from the builder to the immutable
	// collection as a result. Adding more data to the builder after the builder
	// is sealed will result in an error.
	Seal(numTotalDocs int32) closeableTimeField

	// Close closes the builder.
	Close()
}

var (
	errTimeFieldBuilderAlreadyClosed = errors.New("time field builder is already closed")
)

type timeField struct {
	docIDSet DocIDSet
	values   closeableTimeValues
}

func (sf *timeField) DocIDSet() DocIDSet { return sf.docIDSet }
func (sf *timeField) Values() TimeValues { return sf.values }
func (sf *timeField) Close()             { sf.values.Close() }

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

func (b *builderOfTimeField) Snapshot() closeableTimeField {
	docIDSetSnapshot := b.dsb.Snapshot()
	timeValuesSnapshot := b.svb.Snapshot()
	return &timeField{docIDSet: docIDSetSnapshot, values: timeValuesSnapshot}
}

func (b *builderOfTimeField) Seal(numTotalDocs int32) closeableTimeField {
	sealed := &timeField{
		docIDSet: b.dsb.Seal(numTotalDocs),
		values:   b.svb.Seal(),
	}

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
