package storage

import (
	"github.com/xichen2020/eventdb/event/field"

	"github.com/pilosa/pilosa/roaring"
)

const (
	defaultFieldValuesCapacity = 64
)

// fieldWriter writes the value for a given field.
// NB: The docIDs passed in to field writer must be always monotonically increasing.
// TODO(xichen): Investigate the overhead of using roaring bitmap for sparse fields.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
// TODO(xichen): Perhaps pool the type-specific writers and the roaring bitmaps.
type fieldWriter struct {
	path []string
	nw   *nullValueWriter
	bw   *boolValueWriter
	iw   *intValueWriter
	dw   *doubleValueWriter
	sw   *stringValueWriter
}

func newFieldWriter(path []string) *fieldWriter {
	return &fieldWriter{
		path: path,
	}
}

func (w *fieldWriter) addValue(docID int32, v field.Value) {
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

func (w *fieldWriter) addNull(docID int32) {
	if w.nw == nil {
		w.nw = newNullValueWriter()
	}
	w.nw.add(docID)
}

func (w *fieldWriter) addBool(docID int32, val bool) {
	if w.bw == nil {
		w.bw = newBoolValueWriter()
	}
	w.bw.add(docID, val)
}

func (w *fieldWriter) addInt(docID int32, val int) {
	if w.iw == nil {
		w.iw = newIntValueWriter()
	}
	w.iw.add(docID, val)
}

func (w *fieldWriter) addDouble(docID int32, val float64) {
	if w.dw == nil {
		w.dw = newDoubleValueWriter()
	}
	w.dw.add(docID, val)
}

func (w *fieldWriter) addString(docID int32, val string) {
	if w.sw == nil {
		w.sw = newStringValueWriter()
	}
	w.sw.add(docID, val)
}

type nullValueWriter struct {
	docIDs *roaring.Bitmap
}

func newNullValueWriter() *nullValueWriter {
	return &nullValueWriter{
		docIDs: roaring.NewBitmap(),
	}
}

func (nw *nullValueWriter) add(docID int32) {
	nw.docIDs.Add(uint64(docID))
}

type boolValueWriter struct {
	docIDs *roaring.Bitmap
	vals   []bool
}

func newBoolValueWriter() *boolValueWriter {
	return &boolValueWriter{
		docIDs: roaring.NewBitmap(),
		vals:   make([]bool, 0, defaultFieldValuesCapacity),
	}
}

func (nw *boolValueWriter) add(docID int32, val bool) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = append(nw.vals, val)
}

type intValueWriter struct {
	docIDs *roaring.Bitmap
	vals   []int
}

func newIntValueWriter() *intValueWriter {
	return &intValueWriter{
		docIDs: roaring.NewBitmap(),
		vals:   make([]int, 0, defaultFieldValuesCapacity),
	}
}

func (nw *intValueWriter) add(docID int32, val int) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = append(nw.vals, val)
}

type doubleValueWriter struct {
	docIDs *roaring.Bitmap
	vals   []float64
}

func newDoubleValueWriter() *doubleValueWriter {
	return &doubleValueWriter{
		docIDs: roaring.NewBitmap(),
		vals:   make([]float64, 0, defaultFieldValuesCapacity),
	}
}

func (nw *doubleValueWriter) add(docID int32, val float64) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = append(nw.vals, val)
}

type stringValueWriter struct {
	docIDs *roaring.Bitmap
	vals   []string
}

func newStringValueWriter() *stringValueWriter {
	return &stringValueWriter{
		docIDs: roaring.NewBitmap(),
		vals:   make([]string, 0, defaultFieldValuesCapacity),
	}
}

func (nw *stringValueWriter) add(docID int32, val string) {
	nw.docIDs.Add(uint64(docID))
	nw.vals = append(nw.vals, val)
}
