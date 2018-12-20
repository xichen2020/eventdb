package document

import (
	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/pilosa/pilosa/roaring"
)

// DocsField contains a set of field values and associated doc IDs for a given field.
type DocsField interface {
	// FieldPath returns the field path.
	FieldPath() []string

	// NullIter returns true and the doc ID set if there are boolean values
	// for this field, or false otherwise.
	NullIter() (DocIDSet, bool)

	// BoolIter returns true and the doc ID set alongside value iterator if there are
	// boolean values for this field, or false otherwise.
	BoolIter() (DocIDSet, encoding.RewindableBoolIterator, bool)

	// IntIter returns true and the doc ID set alongside value iterator if there are
	// int values for this field, or false otherwise.
	IntIter() (DocIDSet, encoding.RewindableIntIterator, bool)

	// DoubleIter returns true and the doc ID set alongside value iterator if there are
	// double values for this field, or false otherwise.
	DoubleIter() (DocIDSet, encoding.RewindableDoubleIterator, bool)

	// StringIter returns true and the doc ID set alongside value iterator if there are
	// string values for this field, or false otherwise.
	StringIter() (DocIDSet, encoding.RewindableStringIterator, bool)

	// TimeIter returns true and the the doc ID set alongside value iterator if there
	// are timestamp values for this field, or false otherwise.
	TimeIter() (DocIDSet, encoding.RewindableTimeIterator, bool)
}

// DocsFieldBuilder builds a collection of field values.
type DocsFieldBuilder interface {
	// Add adds a value with its document ID.
	Add(docID int32, v field.ValueUnion)

	// Build returns a readonly field values collection.
	Build(numTotalDocs int) DocsField

	// Close closes the builder.
	Close()
}

// FieldSnapshot is a snapshot of a field containing a collection of values and
// the associated document IDs across many documents.
type FieldSnapshot struct {
	fieldPath []string

	nit *docIDSetWithNullValuesIter
	bit *docIDSetWithBoolValuesIter
	iit *docIDSetWithIntValuesIter
	dit *docIDSetWithDoubleValuesIter
	sit *docIDSetWithStringValuesIter
	tit *docIDSetWithTimeValuesIter
}

// FieldPath returns the field path.
func (f *FieldSnapshot) FieldPath() []string { return f.fieldPath }

// NullIter returns the doc ID iterator if applicable.
func (f *FieldSnapshot) NullIter() (DocIDSet, bool) {
	if f.nit == nil {
		return nil, false
	}
	return f.nit.docIDSet, true
}

// BoolIter returns the boolean value iterator if applicable.
func (f *FieldSnapshot) BoolIter() (DocIDSet, encoding.RewindableBoolIterator, bool) {
	if f.bit == nil {
		return nil, nil, false
	}
	return f.bit.docIDSet, f.bit.valueIter, true
}

// IntIter returns the int value iterator if applicable, or nil otherwise.
func (f *FieldSnapshot) IntIter() (DocIDSet, encoding.RewindableIntIterator, bool) {
	if f.iit == nil {
		return nil, nil, false
	}
	return f.iit.docIDSet, f.iit.valueIter, true
}

// DoubleIter returns the double value iterator if applicable, or nil otherwise.
func (f *FieldSnapshot) DoubleIter() (DocIDSet, encoding.RewindableDoubleIterator, bool) {
	if f.dit == nil {
		return nil, nil, false
	}
	return f.dit.docIDSet, f.dit.valueIter, true
}

// StringIter returns the string value iterator if applicable, or nil otherwise.
func (f *FieldSnapshot) StringIter() (DocIDSet, encoding.RewindableStringIterator, bool) {
	if f.sit == nil {
		return nil, nil, false
	}
	return f.sit.docIDSet, f.sit.valueIter, true
}

// TimeIter returns the time value iterator if applicable, or nil otherwise.
func (f *FieldSnapshot) TimeIter() (DocIDSet, encoding.RewindableTimeIterator, bool) {
	if f.tit == nil {
		return nil, nil, false
	}
	return f.tit.docIDSet, f.tit.valueIter, true
}

// FieldBuilder collects values with associated doc IDs for a given field.
type FieldBuilder struct {
	fieldPath []string
	opts      *FieldBuilderOptions

	closed bool
	nv     *docIDSetBuilderWithNullValues
	bv     *docIDSetBuilderWithBoolValues
	iv     *docIDSetBuilderWithIntValues
	dv     *docIDSetBuilderWithDoubleValues
	sv     *docIDSetBuilderWithStringValues
	tv     *docIDSetBuilderWithTimeValues
}

// NewFieldBuilder creates a new field builder.
func NewFieldBuilder(fieldPath []string, opts *FieldBuilderOptions) *FieldBuilder {
	if opts == nil {
		opts = NewFieldBuilderOptions()
	}
	return &FieldBuilder{
		fieldPath: fieldPath,
		opts:      opts,
	}
}

// Add adds a value to the value collection.
func (b *FieldBuilder) Add(docID int32, v field.ValueUnion) {
	switch v.Type {
	case field.NullType:
		b.addNull(docID)
	case field.BoolType:
		b.addBool(docID, v.BoolVal)
	case field.IntType:
		b.addInt(docID, v.IntVal)
	case field.DoubleType:
		b.addDouble(docID, v.DoubleVal)
	case field.StringType:
		b.addString(docID, v.StringVal)
	case field.TimeType:
		b.addTime(docID, v.TimeNanosVal)
	}
}

// Build returns the value build as an immutable set of field values.
func (b *FieldBuilder) Build(numTotalDocs int) DocsField {
	pathClone := make([]string, len(b.fieldPath))
	copy(pathClone, b.fieldPath)

	var (
		nit *docIDSetWithNullValuesIter
		bit *docIDSetWithBoolValuesIter
		iit *docIDSetWithIntValuesIter
		dit *docIDSetWithDoubleValuesIter
		sit *docIDSetWithStringValuesIter
		tit *docIDSetWithTimeValuesIter
	)
	if b.nv != nil {
		nit = b.nv.Build(numTotalDocs)
	}
	if b.bv != nil {
		bit = b.bv.Build(numTotalDocs)
	}
	if b.iv != nil {
		iit = b.iv.Build(numTotalDocs)
	}
	if b.dv != nil {
		dit = b.dv.Build(numTotalDocs)
	}
	if b.sv != nil {
		sit = b.sv.Build(numTotalDocs)
	}
	if b.tv != nil {
		tit = b.tv.Build(numTotalDocs)
	}
	return &FieldSnapshot{
		fieldPath: pathClone,
		nit:       nit,
		bit:       bit,
		iit:       iit,
		dit:       dit,
		sit:       sit,
		tit:       tit,
	}
}

// Close closes the value builder.
func (b *FieldBuilder) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.nv != nil {
		b.nv.Close()
		b.nv = nil
	}
	if b.bv != nil {
		b.bv.Close()
		b.bv = nil
	}
	if b.iv != nil {
		b.iv.Close()
		b.iv = nil
	}
	if b.dv != nil {
		b.dv.Close()
		b.dv = nil
	}
	if b.sv != nil {
		b.sv.Close()
		b.sv = nil
	}
	if b.tv != nil {
		b.tv.Close()
		b.tv = nil
	}
}

func (b *FieldBuilder) newDocIDSetBuilder() DocIDSetBuilder {
	if b.opts.IsMandatoryField() {
		// MandatoryField is present in every document and as such will always
		// have a full doc ID set.
		return fullDocIDSetBuilder{}
	}
	return newBitmapBasedDocIDSetBuilder(roaring.NewBitmap())
}

func (b *FieldBuilder) addNull(docID int32) {
	if b.nv == nil {
		docIDSet := b.newDocIDSetBuilder()
		b.nv = newDocIDSetBuilderWithNullValues(docIDSet)
	}
	b.nv.Add(docID)
}

func (b *FieldBuilder) addBool(docID int32, v bool) {
	if b.bv == nil {
		docIDSet := b.newDocIDSetBuilder()
		boolValues := newArrayBasedBoolValues(b.opts.BoolArrayPool())
		b.bv = newDocIDSetBuilderWithBoolValues(docIDSet, boolValues)
	}
	b.bv.Add(docID, v)
}

func (b *FieldBuilder) addInt(docID int32, v int) {
	if b.iv == nil {
		docIDSet := b.newDocIDSetBuilder()
		intValues := newArrayBasedIntValues(b.opts.IntArrayPool())
		b.iv = newDocIDSetBuilderWithIntValues(docIDSet, intValues)
	}
	b.iv.Add(docID, v)
}

func (b *FieldBuilder) addDouble(docID int32, v float64) {
	if b.dv == nil {
		docIDSet := b.newDocIDSetBuilder()
		doubleValues := newArrayBasedDoubleValues(b.opts.DoubleArrayPool())
		b.dv = newDocIDSetBuilderWithDoubleValues(docIDSet, doubleValues)
	}
	b.dv.Add(docID, v)
}

func (b *FieldBuilder) addString(docID int32, v string) {
	if b.sv == nil {
		docIDSet := b.newDocIDSetBuilder()
		stringValues := newArrayBasedStringValues(b.opts.StringArrayPool())
		b.sv = newDocIDSetBuilderWithStringValues(docIDSet, stringValues)
	}
	b.sv.Add(docID, v)
}

func (b *FieldBuilder) addTime(docID int32, v int64) {
	if b.tv == nil {
		docIDSet := b.newDocIDSetBuilder()
		timeValues := newArrayBasedTimeValues(b.opts.Int64ArrayPool())
		b.tv = newDocIDSetBuilderWithTimeValues(docIDSet, timeValues)
	}
	b.tv.Add(docID, v)
}

const (
	defaultInitialFieldValuesCapacity = 64
	defaultIsMandatoryField           = false
)

// FieldBuilderOptions provide a set of options for the field builder.
type FieldBuilderOptions struct {
	isMandatoryField bool
	boolArrayPool    *pool.BucketizedBoolArrayPool
	intArrayPool     *pool.BucketizedIntArrayPool
	doubleArrayPool  *pool.BucketizedFloat64ArrayPool
	stringArrayPool  *pool.BucketizedStringArrayPool
	int64ArrayPool   *pool.BucketizedInt64ArrayPool
}

// NewFieldBuilderOptions creates a new set of field builder options.
func NewFieldBuilderOptions() *FieldBuilderOptions {
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

	return &FieldBuilderOptions{
		isMandatoryField: defaultIsMandatoryField,
		boolArrayPool:    boolArrayPool,
		intArrayPool:     intArrayPool,
		doubleArrayPool:  doubleArrayPool,
		stringArrayPool:  stringArrayPool,
		int64ArrayPool:   int64ArrayPool,
	}
}

// SetIsMandatoryField sets if the field is a mandatory field.
func (o *FieldBuilderOptions) SetIsMandatoryField(v bool) *FieldBuilderOptions {
	opts := *o
	opts.isMandatoryField = v
	return &opts
}

// IsMandatoryField returns if the field is a mandatory field.
func (o *FieldBuilderOptions) IsMandatoryField() bool {
	return o.isMandatoryField
}

// SetBoolArrayPool sets the bool array pool.
func (o *FieldBuilderOptions) SetBoolArrayPool(v *pool.BucketizedBoolArrayPool) *FieldBuilderOptions {
	opts := *o
	opts.boolArrayPool = v
	return &opts
}

// BoolArrayPool returns the bool array pool.
func (o *FieldBuilderOptions) BoolArrayPool() *pool.BucketizedBoolArrayPool {
	return o.boolArrayPool
}

// SetIntArrayPool sets the int array pool.
func (o *FieldBuilderOptions) SetIntArrayPool(v *pool.BucketizedIntArrayPool) *FieldBuilderOptions {
	opts := *o
	opts.intArrayPool = v
	return &opts
}

// IntArrayPool returns the int array pool.
func (o *FieldBuilderOptions) IntArrayPool() *pool.BucketizedIntArrayPool {
	return o.intArrayPool
}

// SetDoubleArrayPool sets the double array pool.
func (o *FieldBuilderOptions) SetDoubleArrayPool(v *pool.BucketizedFloat64ArrayPool) *FieldBuilderOptions {
	opts := *o
	opts.doubleArrayPool = v
	return &opts
}

// DoubleArrayPool returns the double array pool.
func (o *FieldBuilderOptions) DoubleArrayPool() *pool.BucketizedFloat64ArrayPool {
	return o.doubleArrayPool
}

// SetStringArrayPool sets the string array pool.
func (o *FieldBuilderOptions) SetStringArrayPool(v *pool.BucketizedStringArrayPool) *FieldBuilderOptions {
	opts := *o
	opts.stringArrayPool = v
	return &opts
}

// StringArrayPool returns the string array pool.
func (o *FieldBuilderOptions) StringArrayPool() *pool.BucketizedStringArrayPool {
	return o.stringArrayPool
}

// SetInt64ArrayPool sets the int64 array pool.
func (o *FieldBuilderOptions) SetInt64ArrayPool(v *pool.BucketizedInt64ArrayPool) *FieldBuilderOptions {
	opts := *o
	opts.int64ArrayPool = v
	return &opts
}

// Int64ArrayPool returns the int64 array pool.
func (o *FieldBuilderOptions) Int64ArrayPool() *pool.BucketizedInt64ArrayPool {
	return o.int64ArrayPool
}
