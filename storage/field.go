package storage

import (
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/pilosa/pilosa/roaring"
)

const (
	defaultFieldDocValuesCapacity = 64
)

// fieldDocValues stores a collection of the (docID, value) pairs for a given field.
// NB: The docIDs passed in to field writer must be always monotonically increasing.
// TODO(xichen): Investigate the overhead of using roaring bitmap for sparse fields.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
// TODO(xichen): Perhaps pool the type-specific writers and the roaring bitmaps.
type fieldDocValues struct {
	path []string
	opts *Options

	nw *nullValues
	bw *boolValues
	iw *intValues
	dw *doubleValues
	sw *stringValues
}

func newFieldDocValues(path []string, opts *Options) *fieldDocValues {
	return &fieldDocValues{
		path: path,
		opts: opts,
	}
}

func (w *fieldDocValues) addValue(docID int32, v field.ValueUnion) {
	switch v.Type {
	case field.NullType:
		w.addNull(docID)
	case field.BoolType:
		w.addBool(docID, v.BoolVal)
	case field.IntType:
		w.addInt(docID, v.IntVal)
	case field.DoubleType:
		w.addDouble(docID, v.DoubleVal)
	case field.StringType:
		w.addString(docID, v.StringVal)
	}
}

func (w *fieldDocValues) addNull(docID int32) {
	if w.nw == nil {
		w.nw = newNullValueWriter()
	}
	w.nw.add(docID)
}

func (w *fieldDocValues) addBool(docID int32, val bool) {
	if w.bw == nil {
		w.bw = newBoolValueWriter(w.opts.BoolArrayPool())
	}
	w.bw.add(docID, val)
}

func (w *fieldDocValues) addInt(docID int32, val int) {
	if w.iw == nil {
		w.iw = newIntValueWriter(w.opts.IntArrayPool())
	}
	w.iw.add(docID, val)
}

func (w *fieldDocValues) addDouble(docID int32, val float64) {
	if w.dw == nil {
		w.dw = newDoubleValueWriter(w.opts.DoubleArrayPool())
	}
	w.dw.add(docID, val)
}

func (w *fieldDocValues) addString(docID int32, val string) {
	if w.sw == nil {
		w.sw = newStringValueWriter(w.opts.StringArrayPool())
	}
	w.sw.add(docID, val)
}

func (w *fieldDocValues) flush(persistFns persist.Fns) error {
	if w.nw != nil {
		if err := persistFns.WriteNullField(w.path, w.nw.docIDs); err != nil {
			return err
		}
	}
	if w.bw != nil {
		if err := persistFns.WriteBoolField(w.path, w.bw.docIDs, w.bw.vals); err != nil {
			return err
		}
	}
	if w.iw != nil {
		if err := persistFns.WriteIntField(w.path, w.iw.docIDs, w.iw.vals); err != nil {
			return err
		}
	}
	if w.dw != nil {
		if err := persistFns.WriteDoubleField(w.path, w.dw.docIDs, w.dw.vals); err != nil {
			return err
		}
	}
	if w.sw != nil {
		if err := persistFns.WriteStringField(w.path, w.sw.docIDs, w.sw.vals); err != nil {
			return err
		}
	}
	return nil
}

func (w *fieldDocValues) close() {
	w.path = nil
	if w.nw != nil {
		w.nw.close()
		w.nw = nil
	}
	if w.bw != nil {
		w.bw.close()
		w.bw = nil
	}
	if w.iw != nil {
		w.iw.close()
		w.iw = nil
	}
	if w.dw != nil {
		w.dw.close()
		w.dw = nil
	}
	if w.sw != nil {
		w.sw.close()
		w.sw = nil
	}
}

type nullValues struct {
	docIDs *roaring.Bitmap
}

func newNullValueWriter() *nullValues {
	return &nullValues{
		docIDs: roaring.NewBitmap(),
	}
}

func (nw *nullValues) add(docID int32) {
	nw.docIDs.Add(uint64(docID))
}

func (nw *nullValues) close() {
	nw.docIDs = nil
}

type boolValues struct {
	boolArrayPool *pool.BucketizedBoolArrayPool
	docIDs        *roaring.Bitmap
	vals          []bool
}

func newBoolValueWriter(boolArrayPool *pool.BucketizedBoolArrayPool) *boolValues {
	return &boolValues{
		boolArrayPool: boolArrayPool,
		docIDs:        roaring.NewBitmap(),
		vals:          boolArrayPool.Get(defaultFieldDocValuesCapacity),
	}
}

func (nw *boolValues) add(docID int32, val bool) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = pool.AppendBool(nw.vals, val, nw.boolArrayPool)
}

func (nw *boolValues) close() {
	nw.docIDs = nil
	nw.vals = nw.vals[:0]
	nw.boolArrayPool.Put(nw.vals, cap(nw.vals))
	nw.vals = nil
}

type intValues struct {
	intArrayPool *pool.BucketizedIntArrayPool
	docIDs       *roaring.Bitmap
	vals         []int
}

func newIntValueWriter(intArrayPool *pool.BucketizedIntArrayPool) *intValues {
	return &intValues{
		intArrayPool: intArrayPool,
		docIDs:       roaring.NewBitmap(),
		vals:         intArrayPool.Get(defaultFieldDocValuesCapacity),
	}
}

func (nw *intValues) add(docID int32, val int) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = pool.AppendInt(nw.vals, val, nw.intArrayPool)
}

func (nw *intValues) close() {
	nw.docIDs = nil
	nw.vals = nw.vals[:0]
	nw.intArrayPool.Put(nw.vals, cap(nw.vals))
	nw.vals = nil
}

type doubleValues struct {
	doubleArrayPool *pool.BucketizedFloat64ArrayPool
	docIDs          *roaring.Bitmap
	vals            []float64
}

func newDoubleValueWriter(doubleArrayPool *pool.BucketizedFloat64ArrayPool) *doubleValues {
	return &doubleValues{
		doubleArrayPool: doubleArrayPool,
		docIDs:          roaring.NewBitmap(),
		vals:            doubleArrayPool.Get(defaultFieldDocValuesCapacity),
	}
}

func (nw *doubleValues) add(docID int32, val float64) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = pool.AppendFloat64(nw.vals, val, nw.doubleArrayPool)
}

func (nw *doubleValues) close() {
	nw.docIDs = nil
	nw.vals = nw.vals[:0]
	nw.doubleArrayPool.Put(nw.vals, cap(nw.vals))
	nw.vals = nil
}

type stringValues struct {
	stringArrayPool *pool.BucketizedStringArrayPool
	docIDs          *roaring.Bitmap
	vals            []string
}

func newStringValueWriter(stringArrayPool *pool.BucketizedStringArrayPool) *stringValues {
	return &stringValues{
		stringArrayPool: stringArrayPool,
		docIDs:          roaring.NewBitmap(),
		vals:            stringArrayPool.Get(defaultFieldDocValuesCapacity),
	}
}

func (nw *stringValues) add(docID int32, val string) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = pool.AppendString(nw.vals, val, nw.stringArrayPool)
}

func (nw *stringValues) close() {
	nw.docIDs = nil
	nw.vals = nw.vals[:0]
	nw.stringArrayPool.Put(nw.vals, cap(nw.vals))
	nw.vals = nil
}
