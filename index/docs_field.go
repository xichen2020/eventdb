package index

import (
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/pool"
	"github.com/xichen2020/eventdb/x/refcnt"

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

	// StringField returns the field subset that has string values, or false otherwise.
	// The string field remains valid until the docs field is closed.
	StringField() (StringField, bool)

	// TimeField returns the field subset that has time values, or false otherwise.
	// The time field remains valid until the docs field is closed.
	TimeField() (TimeField, bool)

	// ShallowCopy returns a shallow copy of the docs field sharing access to the
	// underlying resources and typed fields. As such the resources held will not
	// be released until both the shallow copy and the original owner are closed.
	ShallowCopy() DocsField

	// Close closes the field. It will also releases any resources held iff there is
	// no one holding references to the field.
	Close()
}

// MutableDocsField is a mutable field containing one or more types of field values
// and associated doc IDs across multiple documents for a given field.
type MutableDocsField interface {
	DocsField
}

// DocsFieldBuilder builds a collection of field values.
type DocsFieldBuilder interface {
	// Add adds a value with its document ID.
	Add(docID int32, v field.ValueUnion) error

	// Snapshot returns an immutable snapshot of the doc IDs an the field values
	// contained in the field.
	Snapshot() DocsField

	// Seal seals and closes the builder and returns an immutable docs field that contains (and
	// owns) all the doc IDs and the field values accummulated across `numTotalDocs`
	// documents for this field thus far. The resource ownership is transferred from the
	// builder to the immutable collection as a result. Adding more data to the builder
	// after the builder is sealed will result in an error.
	Seal(numTotalDocs int32) DocsField

	// Close closes the builder.
	Close()
}

var (
	errDocsFieldBuilderAlreadyClosed = errors.New("docs field builder is already closed")
)

type docsField struct {
	*refcnt.RefCounter

	fieldPath  []string
	fieldTypes []field.ValueType

	closed bool
	nf     CloseableNullField
	bf     CloseableBoolField
	intf   CloseableIntField
	df     CloseableDoubleField
	sf     CloseableStringField
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
	sf CloseableStringField,
	tf CloseableTimeField,
) DocsField {
	f := &docsField{
		RefCounter: refcnt.NewRefCounter(),
		fieldPath:  fieldPath,
		fieldTypes: fieldTypes,
		nf:         nf,
		bf:         bf,
		intf:       intf,
		df:         df,
		sf:         sf,
		tf:         tf,
	}
	return f
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

func (f *docsField) StringField() (StringField, bool) {
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

func (f *docsField) ShallowCopy() DocsField {
	f.IncRef()
	shallowCopy := *f
	return &shallowCopy
}

func (f *docsField) Close() {
	if f.closed {
		return
	}
	f.closed = true
	if f.DecRef() > 0 {
		return
	}
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

// docsFieldBuilder is a builder of docs field.
type docsFieldBuilder struct {
	fieldPath []string
	opts      *DocsFieldBuilderOptions

	closed bool
	nfb    nullFieldBuilder
	bfb    boolFieldBuilder
	ifb    intFieldBuilder
	dfb    doubleFieldBuilder
	sfb    stringFieldBuilder
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
	case field.StringType:
		return b.addString(docID, v.StringVal)
	case field.TimeType:
		return b.addTime(docID, v.TimeNanosVal)
	default:
		return fmt.Errorf("unknown field value type %v", v.Type)
	}
}

// Snapshot return an immutable snapshot of the builder state.
func (b *docsFieldBuilder) Snapshot() DocsField {
	var (
		fieldTypes = make([]field.ValueType, 0, 6)
		nf         CloseableNullField
		bf         CloseableBoolField
		intf       CloseableIntField
		df         CloseableDoubleField
		sf         CloseableStringField
		tf         CloseableTimeField
	)
	if b.nfb != nil {
		fieldTypes = append(fieldTypes, field.NullType)
		nf = b.nfb.Snapshot()
	}
	if b.bfb != nil {
		fieldTypes = append(fieldTypes, field.BoolType)
		bf = b.bfb.Snapshot()
	}
	if b.ifb != nil {
		fieldTypes = append(fieldTypes, field.IntType)
		intf = b.ifb.Snapshot()
	}
	if b.dfb != nil {
		fieldTypes = append(fieldTypes, field.DoubleType)
		df = b.dfb.Snapshot()
	}
	if b.sfb != nil {
		fieldTypes = append(fieldTypes, field.StringType)
		sf = b.sfb.Snapshot()
	}
	if b.tfb != nil {
		fieldTypes = append(fieldTypes, field.TimeType)
		tf = b.tfb.Snapshot()
	}

	return NewDocsField(b.fieldPath, fieldTypes, nf, bf, intf, df, sf, tf)
}

// Seal seals the builder.
func (b *docsFieldBuilder) Seal(numTotalDocs int32) DocsField {
	var (
		fieldTypes = make([]field.ValueType, 0, 6)
		nf         CloseableNullField
		bf         CloseableBoolField
		intf       CloseableIntField
		df         CloseableDoubleField
		sf         CloseableStringField
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
		fieldTypes = append(fieldTypes, field.StringType)
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

func (b *docsFieldBuilder) newDocIDSetBuilder() docIDSetBuilder {
	return newBitmapBasedDocIDSetBuilder(roaring.NewBitmap())
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
		boolValuesBuilder := newArrayBasedBoolValues(b.opts.BoolArrayPool())
		b.bfb = newBoolFieldBuilder(docIDsBuilder, boolValuesBuilder)
	}
	return b.bfb.Add(docID, v)
}

func (b *docsFieldBuilder) addInt(docID int32, v int) error {
	if b.bfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		intValuesBuilder := newArrayBasedIntValues(b.opts.IntArrayPool())
		b.ifb = newIntFieldBuilder(docIDsBuilder, intValuesBuilder)
	}
	return b.ifb.Add(docID, v)
}

func (b *docsFieldBuilder) addDouble(docID int32, v float64) error {
	if b.dfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		doubleValuesBuilder := newArrayBasedDoubleValues(b.opts.DoubleArrayPool())
		b.dfb = newDoubleFieldBuilder(docIDsBuilder, doubleValuesBuilder)
	}
	return b.dfb.Add(docID, v)
}

func (b *docsFieldBuilder) addString(docID int32, v string) error {
	if b.sfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		stringValuesBuilder := newArrayBasedStringValues(b.opts.StringArrayPool())
		b.sfb = newStringFieldBuilder(docIDsBuilder, stringValuesBuilder)
	}
	return b.sfb.Add(docID, v)
}

func (b *docsFieldBuilder) addTime(docID int32, v int64) error {
	if b.tfb == nil {
		docIDsBuilder := b.newDocIDSetBuilder()
		timeValuesBuilder := newArrayBasedTimeValues(b.opts.Int64ArrayPool())
		b.tfb = newTimeFieldBuilder(docIDsBuilder, timeValuesBuilder)
	}
	return b.tfb.Add(docID, v)
}

const (
	defaultInitialFieldValuesCapacity = 64
)

// DocsFieldBuilderOptions provide a set of options for the field builder.
type DocsFieldBuilderOptions struct {
	boolArrayPool   *pool.BucketizedBoolArrayPool
	intArrayPool    *pool.BucketizedIntArrayPool
	doubleArrayPool *pool.BucketizedFloat64ArrayPool
	stringArrayPool *pool.BucketizedStringArrayPool
	int64ArrayPool  *pool.BucketizedInt64ArrayPool
}

// NewDocsFieldBuilderOptions creates a new set of field builder options.
func NewDocsFieldBuilderOptions() *DocsFieldBuilderOptions {
	boolArrayPool := pool.NewBucketizedBoolArrayPool(nil, nil)
	boolArrayPool.Init(func(capacity int) []bool { return make([]bool, 0, capacity) })

	intArrayPool := pool.NewBucketizedIntArrayPool(nil, nil)
	intArrayPool.Init(func(capacity int) []int { return make([]int, 0, capacity) })

	doubleArrayPool := pool.NewBucketizedFloat64ArrayPool(nil, nil)
	doubleArrayPool.Init(func(capacity int) []float64 { return make([]float64, 0, capacity) })

	stringArrayPool := pool.NewBucketizedStringArrayPool(nil, nil)
	stringArrayPool.Init(func(capacity int) []string { return make([]string, 0, capacity) })

	int64ArrayPool := pool.NewBucketizedInt64ArrayPool(nil, nil)
	int64ArrayPool.Init(func(capacity int) []int64 { return make([]int64, 0, capacity) })

	return &DocsFieldBuilderOptions{
		boolArrayPool:   boolArrayPool,
		intArrayPool:    intArrayPool,
		doubleArrayPool: doubleArrayPool,
		stringArrayPool: stringArrayPool,
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

// SetStringArrayPool sets the string array pool.
func (o *DocsFieldBuilderOptions) SetStringArrayPool(v *pool.BucketizedStringArrayPool) *DocsFieldBuilderOptions {
	opts := *o
	opts.stringArrayPool = v
	return &opts
}

// StringArrayPool returns the string array pool.
func (o *DocsFieldBuilderOptions) StringArrayPool() *pool.BucketizedStringArrayPool {
	return o.stringArrayPool
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
