package field

import (
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/values/impl"
	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/pilosa/pilosa/roaring"
)

// DocsFieldMetadata contains the documents field metadata.
type DocsFieldMetadata struct {
	FieldPath  []string
	FieldTypes []field.ValueType
}

// DocsField is a field containing one or more types of field values and associated doc
// IDs across multiple documents for a given field.
type DocsField interface {
	// Metadata returns the field metadata.
	Metadata() DocsFieldMetadata

	// NullField returns the field subset that has null values, or false otherwise.
	// The null field remains valid until the docs field is closed.
	NullField() (NullField, bool)

	// BoolField returns the field subset that has bool values, or false otherwise.
	// The bool field remains valid until the docs field is closed.
	BoolField() (BoolField, bool)

	// IntField returns the field subset that has int values, or false otherwise.
	// The int field remains valid until the docs field is closed.
	IntField() (IntField, bool)

	// DoubleField returns the field subset that has double values, or false otherwise.
	// The double field remains valid until the docs field is closed.
	DoubleField() (DoubleField, bool)

	// BytesField returns the field subset that has bytes values, or false otherwise.
	// The bytes field remains valid until the docs field is closed.
	BytesField() (BytesField, bool)

	// TimeField returns the field subset that has time values, or false otherwise.
	// The time field remains valid until the docs field is closed.
	TimeField() (TimeField, bool)

	// FieldForType returns the typed field for a given type, or false otherwise.
	// The typed field remains valid until the docs field is closed.
	FieldForType(t field.ValueType) (Union, bool)

	// NewDocsFieldFor returns a new docs field containing a shallow copy of the typed
	// fields (sharing access to the underlying resources) specified in the given value
	// type set. If a given type does not exist in the current field, it is added to
	// the value type set returned. If a field type is invalid, an error is returned.
	NewDocsFieldFor(fieldTypes field.ValueTypeSet) (DocsField, field.ValueTypeSet, error)

	// ShallowCopy returns a shallow copy of the docs field sharing access to the
	// underlying resources and typed fields. As such the resources held will not
	// be released until both the shallow copy and the original owner are closed.
	ShallowCopy() DocsField

	// NewMergedDocsField creates a new merged docs field by merging the fields in the current
	// docs field and the other docs field, with the current docs field taking precedence for
	// a given field type if both the current docs field and the other docs field has the corresponding
	// field. Both the current docs field and the other docs field remains valid after the call.
	NewMergedDocsField(other DocsField) DocsField

	// Filter applies the given filter against the different types of fields in the docs field,
	// returning a doc ID set iterator that returns the documents matching the filter.
	Filter(
		op filter.Op,
		filterValue *field.ValueUnion,
		numTotalDocs int32,
	) (index.DocIDSetIterator, error)

	// Close closes the field. It will also releases any resources held iff there is
	// no one holding references to the field.
	Close()

	// Interal APIs used within the package.

	// closeableNullField returns a closeable field subset that has null values, or false otherwise.
	// The null field remains valid until the docs field is closed.
	closeableNullField() (CloseableNullField, bool)

	// closeableBoolField returns a closeable field subset that has bool values, or false otherwise.
	// The bool field remains valid until the docs field is closed.
	closeableBoolField() (CloseableBoolField, bool)

	// closeableIntField returns a closeable field subset that has int values, or false otherwise.
	// The int field remains valid until the docs field is closed.
	closeableIntField() (CloseableIntField, bool)

	// closeableDoubleField returns a closeable field subset that has double values, or false otherwise.
	// The double field remains valid until the docs field is closed.
	closeableDoubleField() (CloseableDoubleField, bool)

	// closeableBytesField returns a closeable field subset that has bytes values, or false otherwise.
	// The bytes field remains valid until the docs field is closed.
	closeableBytesField() (CloseableBytesField, bool)

	// closeableTimeField returns a closeable field subset that has time values, or false otherwise.
	// The time field remains valid until the docs field is closed.
	closeableTimeField() (CloseableTimeField, bool)
}

// DocsFieldBuilder builds a collection of field values.
type DocsFieldBuilder interface {
	// Add adds a value with its document ID.
	Add(docID int32, v field.ValueUnion) error

	// Snapshot return an immutable snapshot of the typed fields for the given
	// value type set, returning the new docs field and any types remaining in
	// the given value type set that's not available in the builder.
	SnapshotFor(fieldTypes field.ValueTypeSet) (DocsField, field.ValueTypeSet, error)

	// Seal seals and closes the builder and returns an immutable docs field that contains (and
	// owns) all the doc IDs and the field values accummulated across `numTotalDocs`
	// documents for this field thus far. The resource ownership is transferred from the
	// builder to the immutable collection as a result. Adding more data to the builder
	// after the builder is sealed will result in an error.
	Seal(numTotalDocs int32) DocsField

	// Close closes the builder.
	Close()
}

// CloseFn closes a field.
type CloseFn func()

var (
	// NilDocsField is a nil docs field that can only be used for filtering.
	NilDocsField DocsField = (*docsField)(nil)

	errDocsFieldBuilderAlreadyClosed = errors.New("docs field builder is already closed")
)

// docsField is an immutable collection of different typed fields at the same field path.
// The internal fields of an `docsField` object remains unchanged until the docs field is closed.
// `docsField` is not thread safe. Concurrent access to `docsField` (e.g., calling reading and
// closing APIs) must be protected with synchronization alternatives.
type docsField struct {
	fieldPath  []string
	fieldTypes []field.ValueType

	closed bool
	nf     CloseableNullField
	bf     CloseableBoolField
	intf   CloseableIntField
	df     CloseableDoubleField
	sf     CloseableBytesField
	tf     CloseableTimeField
}

// NewDocsField creates a new docs field.
func NewDocsField(
	fieldPath []string,
	fieldTypes []field.ValueType,
	nf CloseableNullField,
	bf CloseableBoolField,
	intf CloseableIntField,
	df CloseableDoubleField,
	sf CloseableBytesField,
	tf CloseableTimeField,
) DocsField {
	return &docsField{
		fieldPath:  fieldPath,
		fieldTypes: fieldTypes,
		nf:         nf,
		bf:         bf,
		intf:       intf,
		df:         df,
		sf:         sf,
		tf:         tf,
	}
}

func (f *docsField) Metadata() DocsFieldMetadata {
	return DocsFieldMetadata{
		FieldPath:  f.fieldPath,
		FieldTypes: f.fieldTypes,
	}
}

func (f *docsField) NullField() (NullField, bool) {
	if f.nf == nil {
		return nil, false
	}
	return f.nf, true
}

func (f *docsField) BoolField() (BoolField, bool) {
	if f.bf == nil {
		return nil, false
	}
	return f.bf, true
}

func (f *docsField) IntField() (IntField, bool) {
	if f.intf == nil {
		return nil, false
	}
	return f.intf, true
}

func (f *docsField) DoubleField() (DoubleField, bool) {
	if f.df == nil {
		return nil, false
	}
	return f.df, true
}

func (f *docsField) BytesField() (BytesField, bool) {
	if f.sf == nil {
		return nil, false
	}
	return f.sf, true
}

func (f *docsField) TimeField() (TimeField, bool) {
	if f.tf == nil {
		return nil, false
	}
	return f.tf, true
}

func (f *docsField) FieldForType(t field.ValueType) (Union, bool) {
	switch t {
	case field.NullType:
		if nf, exists := f.NullField(); exists {
			return Union{Type: field.NullType, NullField: nf}, true
		}
	case field.BoolType:
		if bf, exists := f.BoolField(); exists {
			return Union{Type: field.BoolType, BoolField: bf}, true
		}
	case field.IntType:
		if intf, exists := f.IntField(); exists {
			return Union{Type: field.IntType, IntField: intf}, true
		}
	case field.DoubleType:
		if df, exists := f.DoubleField(); exists {
			return Union{Type: field.DoubleType, DoubleField: df}, true
		}
	case field.BytesType:
		if sf, exists := f.BytesField(); exists {
			return Union{Type: field.BytesType, BytesField: sf}, true
		}
	case field.TimeType:
		if tf, exists := f.TimeField(); exists {
			return Union{Type: field.TimeType, TimeField: tf}, true
		}
	}
	return Union{}, false
}

func (f *docsField) NewDocsFieldFor(
	fieldTypes field.ValueTypeSet,
) (DocsField, field.ValueTypeSet, error) {
	var (
		nf             CloseableNullField
		bf             CloseableBoolField
		intf           CloseableIntField
		df             CloseableDoubleField
		sf             CloseableBytesField
		tf             CloseableTimeField
		availableTypes = make([]field.ValueType, 0, len(fieldTypes))
		remainingTypes field.ValueTypeSet
		err            error
	)
	for t := range fieldTypes {
		hasType := true

		switch t {
		case field.NullType:
			if f.nf != nil {
				nf = f.nf.ShallowCopy()
				break
			}
			hasType = false
		case field.BoolType:
			if f.bf != nil {
				bf = f.bf.ShallowCopy()
				break
			}
			hasType = false
		case field.IntType:
			if f.intf != nil {
				intf = f.intf.ShallowCopy()
				break
			}
			hasType = false
		case field.DoubleType:
			if f.df != nil {
				df = f.df.ShallowCopy()
				break
			}
			hasType = false
		case field.BytesType:
			if f.sf != nil {
				sf = f.sf.ShallowCopy()
				break
			}
			hasType = false
		case field.TimeType:
			if f.tf != nil {
				tf = f.tf.ShallowCopy()
				break
			}
			hasType = false
		default:
			err = fmt.Errorf("unknown field type %v", t)
		}

		if err != nil {
			break
		}
		if hasType {
			availableTypes = append(availableTypes, t)
			continue
		}
		if remainingTypes == nil {
			remainingTypes = make(field.ValueTypeSet, len(fieldTypes))
		}
		remainingTypes[t] = struct{}{}
	}

	if err == nil {
		return NewDocsField(f.fieldPath, availableTypes, nf, bf, intf, df, sf, tf), remainingTypes, nil
	}

	// Close all resources on error.
	if nf != nil {
		nf.Close()
	}
	if bf != nil {
		bf.Close()
	}
	if intf != nil {
		intf.Close()
	}
	if df != nil {
		df.Close()
	}
	if sf != nil {
		sf.Close()
	}
	if tf != nil {
		tf.Close()
	}

	return nil, nil, err
}

func (f *docsField) ShallowCopy() DocsField {
	var (
		nf   CloseableNullField
		bf   CloseableBoolField
		intf CloseableIntField
		df   CloseableDoubleField
		sf   CloseableBytesField
		tf   CloseableTimeField
	)
	if f.nf != nil {
		nf = f.nf.ShallowCopy()
	}
	if f.bf != nil {
		bf = f.bf.ShallowCopy()
	}
	if f.intf != nil {
		intf = f.intf.ShallowCopy()
	}
	if f.df != nil {
		df = f.df.ShallowCopy()
	}
	if f.sf != nil {
		sf = f.sf.ShallowCopy()
	}
	if f.tf != nil {
		tf = f.tf.ShallowCopy()
	}
	return NewDocsField(f.fieldPath, f.fieldTypes, nf, bf, intf, df, sf, tf)
}

func (f *docsField) NewMergedDocsField(other DocsField) DocsField {
	if other == nil {
		return f.ShallowCopy()
	}
	var (
		nf     CloseableNullField
		bf     CloseableBoolField
		intf   CloseableIntField
		df     CloseableDoubleField
		sf     CloseableBytesField
		tf     CloseableTimeField
		merged bool
	)

	if f.nf != nil {
		nf = f.nf.ShallowCopy()
	} else if onf, exists := other.closeableNullField(); exists {
		nf = onf.ShallowCopy()
		merged = true
	}

	if f.bf != nil {
		bf = f.bf.ShallowCopy()
	} else if obf, exists := other.closeableBoolField(); exists {
		bf = obf.ShallowCopy()
		merged = true
	}

	if f.intf != nil {
		intf = f.intf.ShallowCopy()
	} else if ointf, exists := other.closeableIntField(); exists {
		intf = ointf.ShallowCopy()
		merged = true
	}

	if f.df != nil {
		df = f.df.ShallowCopy()
	} else if odf, exists := other.closeableDoubleField(); exists {
		df = odf.ShallowCopy()
		merged = true
	}

	if f.sf != nil {
		sf = f.sf.ShallowCopy()
	} else if osf, exists := other.closeableBytesField(); exists {
		sf = osf.ShallowCopy()
		merged = true
	}

	if f.tf != nil {
		tf = f.tf.ShallowCopy()
	} else if otf, exists := other.closeableTimeField(); exists {
		tf = otf.ShallowCopy()
		merged = true
	}

	fieldTypes := f.fieldTypes
	if merged {
		fieldTypes = field.MergeTypes(f.fieldTypes, other.Metadata().FieldTypes)
	}
	return NewDocsField(f.fieldPath, fieldTypes, nf, bf, intf, df, sf, tf)
}

// TODO(xichen): Add filter tests.
func (f *docsField) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
	numTotalDocs int32,
) (index.DocIDSetIterator, error) {
	if f == nil || len(f.fieldTypes) == 0 {
		docIDIter := index.NewEmptyDocIDSetIterator()
		if op.IsDocIDSetFilter() {
			docIDSetIteratorFn := op.MustDocIDSetFilterFn(numTotalDocs)
			return docIDSetIteratorFn(docIDIter), nil
		}
		return docIDIter, nil
	}

	if len(f.fieldTypes) == 1 {
		return f.filterForType(f.fieldTypes[0], op, filterValue, numTotalDocs)
	}

	combinator, err := op.MultiTypeCombinator()
	if err != nil {
		return nil, err
	}
	iters := make([]index.DocIDSetIterator, 0, len(f.fieldTypes))
	for _, t := range f.fieldTypes {
		iter, err := f.filterForType(t, op, filterValue, numTotalDocs)
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	}
	switch combinator {
	case filter.And:
		return index.NewInAllDocIDSetIterator(iters...), nil
	case filter.Or:
		return index.NewInAnyDocIDSetIterator(iters...), nil
	default:
		return nil, fmt.Errorf("unknown filter combinator %v", combinator)
	}
}

// Precondition: Field type `t` is guaranteed to exist in the docs field.
func (f *docsField) filterForType(
	t field.ValueType,
	op filter.Op,
	filterValue *field.ValueUnion,
	numTotalDocs int32,
) (index.DocIDSetIterator, error) {
	switch t {
	case field.NullType:
		return f.nf.Filter(op, filterValue, numTotalDocs)
	case field.BoolType:
		return f.bf.Filter(op, filterValue, numTotalDocs)
	case field.IntType:
		return f.intf.Filter(op, filterValue, numTotalDocs)
	case field.DoubleType:
		return f.df.Filter(op, filterValue, numTotalDocs)
	case field.BytesType:
		return f.sf.Filter(op, filterValue, numTotalDocs)
	case field.TimeType:
		return f.tf.Filter(op, filterValue, numTotalDocs)
	default:
		return nil, fmt.Errorf("unknown field type %v", t)
	}
}

func (f *docsField) Close() {
	if f.closed {
		return
	}
	f.closed = true
	if f.nf != nil {
		f.nf.Close()
		f.nf = nil
	}
	if f.bf != nil {
		f.bf.Close()
		f.bf = nil
	}
	if f.intf != nil {
		f.intf.Close()
		f.intf = nil
	}
	if f.df != nil {
		f.df.Close()
		f.df = nil
	}
	if f.sf != nil {
		f.sf.Close()
		f.sf = nil
	}
	if f.tf != nil {
		f.tf.Close()
		f.tf = nil
	}
}

func (f *docsField) closeableNullField() (CloseableNullField, bool) {
	if f.nf == nil {
		return nil, false
	}
	return f.nf, true
}

func (f *docsField) closeableBoolField() (CloseableBoolField, bool) {
	if f.bf == nil {
		return nil, false
	}
	return f.bf, true
}

func (f *docsField) closeableIntField() (CloseableIntField, bool) {
	if f.intf == nil {
		return nil, false
	}
	return f.intf, true
}

func (f *docsField) closeableDoubleField() (CloseableDoubleField, bool) {
	if f.df == nil {
		return nil, false
	}
	return f.df, true
}

func (f *docsField) closeableBytesField() (CloseableBytesField, bool) {
	if f.sf == nil {
		return nil, false
	}
	return f.sf, true
}

func (f *docsField) closeableTimeField() (CloseableTimeField, bool) {
	if f.tf == nil {
		return nil, false
	}
	return f.tf, true
}

// docsFieldBuilder is a builder of docs field.
type docsFieldBuilder struct {
	fieldPath []string
	opts      *DocsFieldBuilderOptions

	closed bool
	nfb    nullFieldBuilder
	bfb    boolFieldBuilder
	ifb    intFieldBuilder
	dfb    doubleFieldBuilder
	sfb    bytesFieldBuilder
	tfb    timeFieldBuilder
}

// NewDocsFieldBuilder creates a new docs field builder.
func NewDocsFieldBuilder(fieldPath []string, opts *DocsFieldBuilderOptions) DocsFieldBuilder {
	if opts == nil {
		opts = NewDocsFieldBuilderOptions()
	}
	return &docsFieldBuilder{
		fieldPath: fieldPath,
		opts:      opts,
	}
}

// Add adds a value to the value collection.
func (b *docsFieldBuilder) Add(docID int32, v field.ValueUnion) error {
	if b.closed {
		return errDocsFieldBuilderAlreadyClosed
	}
	switch v.Type {
	case field.NullType:
		return b.addNull(docID)
	case field.BoolType:
		return b.addBool(docID, v.BoolVal)
	case field.IntType:
		return b.addInt(docID, v.IntVal)
	case field.DoubleType:
		return b.addDouble(docID, v.DoubleVal)
	case field.BytesType:
		return b.addBytes(docID, v.BytesVal.SafeBytes())
	case field.TimeType:
		return b.addTime(docID, v.TimeNanosVal)
	default:
		return fmt.Errorf("unknown field value type %v", v.Type)
	}
}

func (b *docsFieldBuilder) SnapshotFor(
	fieldTypes field.ValueTypeSet,
) (DocsField, field.ValueTypeSet, error) {
	var (
		nf             CloseableNullField
		bf             CloseableBoolField
		intf           CloseableIntField
		df             CloseableDoubleField
		sf             CloseableBytesField
		tf             CloseableTimeField
		availableTypes = make([]field.ValueType, 0, len(fieldTypes))
		remainingTypes field.ValueTypeSet
		err            error
	)
	for t := range fieldTypes {
		hasType := true

		switch t {
		case field.NullType:
			if b.nfb != nil {
				nf = b.nfb.Snapshot()
				break
			}
			hasType = false
		case field.BoolType:
			if b.bfb != nil {
				bf = b.bfb.Snapshot()
				break
			}
			hasType = false
		case field.IntType:
			if b.ifb != nil {
				intf = b.ifb.Snapshot()
				break
			}
			hasType = false
		case field.DoubleType:
			if b.dfb != nil {
				df = b.dfb.Snapshot()
				break
			}
			hasType = false
		case field.BytesType:
			if b.sfb != nil {
				sf = b.sfb.Snapshot()
				break
			}
			hasType = false
		case field.TimeType:
			if b.tfb != nil {
				tf = b.tfb.Snapshot()
				break
			}
			hasType = false
		default:
			err = fmt.Errorf("unknown field type %v", t)
		}

		if err != nil {
			break
		}
		if hasType {
			availableTypes = append(availableTypes, t)
			continue
		}
		if remainingTypes == nil {
			remainingTypes = make(field.ValueTypeSet, len(fieldTypes))
		}
		remainingTypes[t] = struct{}{}
	}

	if err == nil {
		return NewDocsField(b.fieldPath, availableTypes, nf, bf, intf, df, sf, tf), remainingTypes, nil
	}

	// Close all resources on error.
	if nf != nil {
		nf.Close()
	}
	if bf != nil {
		bf.Close()
	}
	if intf != nil {
		intf.Close()
	}
	if df != nil {
		df.Close()
	}
	if sf != nil {
		sf.Close()
	}
	if tf != nil {
		tf.Close()
	}

	return nil, nil, err
}

// Seal seals the builder.
func (b *docsFieldBuilder) Seal(numTotalDocs int32) DocsField {
	var (
		fieldTypes = make([]field.ValueType, 0, 6)
		nf         CloseableNullField
		bf         CloseableBoolField
		intf       CloseableIntField
		df         CloseableDoubleField
		sf         CloseableBytesField
		tf         CloseableTimeField
	)
	if b.nfb != nil {
		fieldTypes = append(fieldTypes, field.NullType)
		nf = b.nfb.Seal(numTotalDocs)
	}
	if b.bfb != nil {
		fieldTypes = append(fieldTypes, field.BoolType)
		bf = b.bfb.Seal(numTotalDocs)
	}
	if b.ifb != nil {
		fieldTypes = append(fieldTypes, field.IntType)
		intf = b.ifb.Seal(numTotalDocs)
	}
	if b.dfb != nil {
		fieldTypes = append(fieldTypes, field.DoubleType)
		df = b.dfb.Seal(numTotalDocs)
	}
	if b.sfb != nil {
		fieldTypes = append(fieldTypes, field.BytesType)
		sf = b.sfb.Seal(numTotalDocs)
	}
	if b.tfb != nil {
		fieldTypes = append(fieldTypes, field.TimeType)
		tf = b.tfb.Seal(numTotalDocs)
	}

	// The sealed field shares the same refcounter as the builder because it holds
	// references to the same underlying resources.
	sealed := NewDocsField(b.fieldPath, fieldTypes, nf, bf, intf, df, sf, tf)

	// Clear and close the builder so it's no longer writable.
	*b = docsFieldBuilder{}
	b.Close()

	return sealed
}

// Close closes the value builder.
func (b *docsFieldBuilder) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.nfb != nil {
		b.nfb.Close()
		b.nfb = nil
	}
	if b.bfb != nil {
		b.bfb.Close()
		b.bfb = nil
	}
	if b.ifb != nil {
		b.ifb.Close()
		b.ifb = nil
	}
	if b.dfb != nil {
		b.dfb.Close()
		b.dfb = nil
	}
	if b.sfb != nil {
		b.sfb.Close()
		b.sfb = nil
	}
	if b.tfb != nil {
		b.tfb.Close()
		b.tfb = nil
	}
}

func (b *docsFieldBuilder) newDocIDSetBuilder() index.DocIDSetBuilder {
	return index.NewBitmapBasedDocIDSetBuilder(roaring.NewBitmap())
}

func (b *docsFieldBuilder) addNull(docID int32) error {
	if b.nfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		b.nfb = newNullFieldBuilder(docIDsBuilder)
	}
	return b.nfb.Add(docID)
}

func (b *docsFieldBuilder) addBool(docID int32, v bool) error {
	if b.bfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		boolValuesBuilder := impl.NewArrayBasedBoolValues(b.opts.BoolArrayPool())
		b.bfb = newBoolFieldBuilder(docIDsBuilder, boolValuesBuilder)
	}
	return b.bfb.Add(docID, v)
}

func (b *docsFieldBuilder) addInt(docID int32, v int) error {
	if b.ifb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		intValuesBuilder := impl.NewArrayBasedIntValues(b.opts.IntArrayPool())
		b.ifb = newIntFieldBuilder(docIDsBuilder, intValuesBuilder)
	}
	return b.ifb.Add(docID, v)
}

func (b *docsFieldBuilder) addDouble(docID int32, v float64) error {
	if b.dfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		doubleValuesBuilder := impl.NewArrayBasedDoubleValues(b.opts.DoubleArrayPool())
		b.dfb = newDoubleFieldBuilder(docIDsBuilder, doubleValuesBuilder)
	}
	return b.dfb.Add(docID, v)
}

func (b *docsFieldBuilder) addBytes(docID int32, v []byte) error {
	if b.sfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		bytesValuesBuilder := impl.NewArrayBasedBytesValues(b.opts.BytesArrayPool(), b.opts.BytesArrayResetFn())
		b.sfb = newBytesFieldBuilder(docIDsBuilder, bytesValuesBuilder)
	}
	return b.sfb.Add(docID, v)
}

func (b *docsFieldBuilder) addTime(docID int32, v int64) error {
	if b.tfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		timeValuesBuilder := impl.NewArrayBasedTimeValues(b.opts.Int64ArrayPool())
		b.tfb = newTimeFieldBuilder(docIDsBuilder, timeValuesBuilder)
	}
	return b.tfb.Add(docID, v)
}

// DocsFieldBuilderOptions provide a set of options for the field builder.
type DocsFieldBuilderOptions struct {
	boolArrayPool     *pool.BucketizedBoolArrayPool
	intArrayPool      *pool.BucketizedIntArrayPool
	doubleArrayPool   *pool.BucketizedFloat64ArrayPool
	bytesArrayPool    *pool.BucketizedBytesArrayPool
	int64ArrayPool    *pool.BucketizedInt64ArrayPool
	bytesArrayResetFn bytes.ArrayFn
}

// NewDocsFieldBuilderOptions creates a new set of field builder options.
func NewDocsFieldBuilderOptions() *DocsFieldBuilderOptions {
	boolArrayPool := pool.NewBucketizedBoolArrayPool(nil, nil)
	boolArrayPool.Init(func(capacity int) []bool { return make([]bool, 0, capacity) })

	intArrayPool := pool.NewBucketizedIntArrayPool(nil, nil)
	intArrayPool.Init(func(capacity int) []int { return make([]int, 0, capacity) })

	doubleArrayPool := pool.NewBucketizedFloat64ArrayPool(nil, nil)
	doubleArrayPool.Init(func(capacity int) []float64 { return make([]float64, 0, capacity) })

	bytesArrayPool := pool.NewBucketizedBytesArrayPool(nil, nil)
	bytesArrayPool.Init(func(capacity int) [][]byte { return make([][]byte, 0, capacity) })

	int64ArrayPool := pool.NewBucketizedInt64ArrayPool(nil, nil)
	int64ArrayPool.Init(func(capacity int) []int64 { return make([]int64, 0, capacity) })

	return &DocsFieldBuilderOptions{
		boolArrayPool:   boolArrayPool,
		intArrayPool:    intArrayPool,
		doubleArrayPool: doubleArrayPool,
		bytesArrayPool:  bytesArrayPool,
		int64ArrayPool:  int64ArrayPool,
	}
}

// SetBoolArrayPool sets the bool array pool.
func (o *DocsFieldBuilderOptions) SetBoolArrayPool(v *pool.BucketizedBoolArrayPool) *DocsFieldBuilderOptions {
	opts := *o
	opts.boolArrayPool = v
	return &opts
}

// BoolArrayPool returns the bool array pool.
func (o *DocsFieldBuilderOptions) BoolArrayPool() *pool.BucketizedBoolArrayPool {
	return o.boolArrayPool
}

// SetIntArrayPool sets the int array pool.
func (o *DocsFieldBuilderOptions) SetIntArrayPool(v *pool.BucketizedIntArrayPool) *DocsFieldBuilderOptions {
	opts := *o
	opts.intArrayPool = v
	return &opts
}

// IntArrayPool returns the int array pool.
func (o *DocsFieldBuilderOptions) IntArrayPool() *pool.BucketizedIntArrayPool {
	return o.intArrayPool
}

// SetDoubleArrayPool sets the double array pool.
func (o *DocsFieldBuilderOptions) SetDoubleArrayPool(v *pool.BucketizedFloat64ArrayPool) *DocsFieldBuilderOptions {
	opts := *o
	opts.doubleArrayPool = v
	return &opts
}

// DoubleArrayPool returns the double array pool.
func (o *DocsFieldBuilderOptions) DoubleArrayPool() *pool.BucketizedFloat64ArrayPool {
	return o.doubleArrayPool
}

// SetBytesArrayPool sets the bytes array pool.
func (o *DocsFieldBuilderOptions) SetBytesArrayPool(v *pool.BucketizedBytesArrayPool) *DocsFieldBuilderOptions {
	opts := *o
	opts.bytesArrayPool = v
	return &opts
}

// BytesArrayPool returns the bytes array pool.
func (o *DocsFieldBuilderOptions) BytesArrayPool() *pool.BucketizedBytesArrayPool {
	return o.bytesArrayPool
}

// SetInt64ArrayPool sets the int64 array pool.
func (o *DocsFieldBuilderOptions) SetInt64ArrayPool(v *pool.BucketizedInt64ArrayPool) *DocsFieldBuilderOptions {
	opts := *o
	opts.int64ArrayPool = v
	return &opts
}

// Int64ArrayPool returns the int64 array pool.
func (o *DocsFieldBuilderOptions) Int64ArrayPool() *pool.BucketizedInt64ArrayPool {
	return o.int64ArrayPool
}

// SetBytesArrayResetFn sets a value reset function for bytes values.
func (o *DocsFieldBuilderOptions) SetBytesArrayResetFn(fn bytes.ArrayFn) *DocsFieldBuilderOptions {
	opts := *o
	opts.bytesArrayResetFn = fn
	return &opts
}

// BytesArrayResetFn resets bytes array values before returning a bytes array back to the memory pool.
func (o *DocsFieldBuilderOptions) BytesArrayResetFn() bytes.ArrayFn {
	return o.bytesArrayResetFn
}
