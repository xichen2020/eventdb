package fs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"

	boolenc "github.com/xichen2020/eventdb/encoding/bool"
	doubleenc "github.com/xichen2020/eventdb/encoding/double"
	intenc "github.com/xichen2020/eventdb/encoding/int"
	stringenc "github.com/xichen2020/eventdb/encoding/string"
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/generated/proto/infopb"
	"github.com/xichen2020/eventdb/persist/schema"
	xbytes "github.com/xichen2020/eventdb/x/bytes"

	"github.com/pborman/uuid"
	"github.com/pilosa/pilosa/roaring"
)

// segmentWriter is responsible for writing segments to filesystem.
// TODO(xichen): make docIDs an interface with `WriteTo()` to abstract the details
// of how the doc IDs are encoded.
type segmentWriter interface {
	// Open opens the writer.
	Open(opts writerOpenOptions) error

	// WriteNullField writes a field with docID set and null values.
	WriteNullField(fieldPath []string, docIDs *roaring.Bitmap) error

	// WriteBoolField writes a field with docID set and bool values.
	WriteBoolField(fieldPath []string, docIDs *roaring.Bitmap, vals []bool) error

	// WriteIntField writes a field with docID set and int values.
	WriteIntField(fieldPath []string, docIDs *roaring.Bitmap, vals []int) error

	// WriteDoubleField writes a field with docID set and double values.
	WriteDoubleField(fieldPath []string, docIDs *roaring.Bitmap, vals []float64) error

	// WriteStringField writes a field with docID set and string values.
	WriteStringField(fieldPath []string, docIDs *roaring.Bitmap, vals []string) error

	// Close closes the writer.
	Close() error
}

var (
	errEmptyDocIDSet = errors.New("empty document ID set")
)

// writerOpenOptions provide a set of options for opening a writer.
type writerOpenOptions struct {
	Namespace    []byte
	Shard        uint32
	MinTimeNanos int64
	MaxTimeNanos int64
	NumDocuments int32
}

type writer struct {
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
	fieldSeparator   string

	bufWriter  *bufio.Writer
	info       *infopb.SegmentInfo
	segmentDir string
	buf        []byte
	bytesBuf   bytes.Buffer

	bw boolenc.Encoder
	iw intenc.Encoder
	dw doubleenc.Encoder
	sw stringenc.Encoder

	boolIt   *boolIterator
	intIt    *intIterator
	doubleIt *doubleIterator
	stringIt *stringIterator
	valueIt  valueIteratorUnion

	err error
}

// newSegmentWriter creates a new segment writer.
// TODO(xichen): Initialize the type-specific encoders.
// TODO(xichen): Compute checksum for info file and data file in a streaming fashion.
// TODO(xichen): Encode timestamp field and source field differently.
// TODO(xichen): Encode timestamp with configurable precision.
// TODO(xichen): Investigate the benefit of writing a single field file.
func newSegmentWriter(opts *Options) segmentWriter {
	w := &writer{
		filePathPrefix:   opts.FilePathPrefix(),
		newFileMode:      opts.NewFileMode(),
		newDirectoryMode: opts.NewDirectoryMode(),
		fieldSeparator:   opts.FieldSeparator(),
		bufWriter:        bufio.NewWriterSize(nil, opts.WriteBufferSize()),
		info:             &infopb.SegmentInfo{},
		boolIt:           newBoolIterator(nil),
		intIt:            newIntIterator(nil),
		doubleIt:         newDoubleIterator(nil),
		stringIt:         newStringIterator(nil),
	}
	w.valueIt = valueIteratorUnion{
		boolIt:   w.boolIt,
		intIt:    w.intIt,
		doubleIt: w.doubleIt,
		stringIt: w.stringIt,
	}
	return w
}

func (w *writer) Open(opts writerOpenOptions) error {
	var (
		namespace    = opts.Namespace
		shard        = opts.Shard
		minTimeNanos = opts.MinTimeNanos
		maxTimeNanos = opts.MaxTimeNanos
		segmentID    = uuid.New()[:8]
	)

	shardDir := shardDataDirPath(w.filePathPrefix, namespace, shard)
	segmentDir := segmentDirPath(shardDir, minTimeNanos, maxTimeNanos, segmentID)
	if err := os.MkdirAll(segmentDir, w.newDirectoryMode); err != nil {
		return err
	}
	w.segmentDir = segmentDir
	w.err = nil

	w.info.Reset()
	w.info.Version = schema.SegmentVersion
	w.info.MinTimestampNanos = opts.MinTimeNanos
	w.info.MaxTimestampNanos = opts.MaxTimeNanos
	w.info.NumDocuments = opts.NumDocuments
	return w.writeInfoFile()
}

func (w *writer) WriteNullField(fieldPath []string, docIDs *roaring.Bitmap) error {
	w.valueIt.valueType = field.NullType
	return w.writeFieldDataFile(fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteBoolField(fieldPath []string, docIDs *roaring.Bitmap, vals []bool) error {
	w.boolIt.Reset(vals)
	w.valueIt.valueType = field.BoolType
	return w.writeFieldDataFile(fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteIntField(fieldPath []string, docIDs *roaring.Bitmap, vals []int) error {
	w.intIt.Reset(vals)
	w.valueIt.valueType = field.IntType
	return w.writeFieldDataFile(fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteDoubleField(fieldPath []string, docIDs *roaring.Bitmap, vals []float64) error {
	w.doubleIt.Reset(vals)
	w.valueIt.valueType = field.DoubleType
	return w.writeFieldDataFile(fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteStringField(fieldPath []string, docIDs *roaring.Bitmap, vals []string) error {
	w.stringIt.Reset(vals)
	w.valueIt.valueType = field.StringType
	return w.writeFieldDataFile(fieldPath, docIDs, w.valueIt)
}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}
	// NB(xichen): only write out the checkpoint file if there are no errors
	// encountered between calling writer.Open() and writer.Close().
	if err := w.writeCheckpointFile(); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) writeInfoFile() error {
	path := infoFilePath(w.segmentDir)
	f, err := w.openWritable(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w.bufWriter.Reset(f)
	msgSize := w.info.Size()
	payloadSize := maxMessageSizeInBytes + msgSize
	w.ensureBufferSize(payloadSize)
	size := binary.PutVarint(w.buf, int64(msgSize))
	n, err := w.info.MarshalTo(w.buf[size:])
	if err != nil {
		return err
	}
	size += n
	_, err = w.bufWriter.Write(w.buf[:size])
	if err != nil {
		return err
	}
	return w.bufWriter.Flush()
}

func (w *writer) writeFieldDataFile(
	fieldPath []string,
	docIDs *roaring.Bitmap,
	valueIt valueIteratorUnion,
) error {
	if w.err != nil {
		return w.err
	}
	w.err = w.writeFieldDataFileInternal(fieldPath, docIDs, valueIt)
	return w.err
}

func (w *writer) writeFieldDataFileInternal(
	fieldPath []string,
	docIDs *roaring.Bitmap,
	valueIt valueIteratorUnion,
) error {
	if docIDs.Count() == 0 {
		return errEmptyDocIDSet
	}
	path := fieldDataFilePath(w.segmentDir, fieldPath, w.fieldSeparator, &w.bytesBuf)
	f, err := w.openWritable(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header.
	w.bufWriter.Reset(f)
	_, err = w.bufWriter.Write(magicHeader)
	if err != nil {
		return err
	}

	// Write doc ID set.
	// TODO(xichen): Precompute the size of the encoded bitmap to avoid the memory
	// cost of writing the encoded bitmap to a byte buffer then to file.
	w.bytesBuf.Reset()
	_, err = docIDs.WriteTo(&w.bytesBuf)
	if err != nil {
		return err
	}
	w.ensureBufferSize(maxMessageSizeInBytes)
	size := binary.PutVarint(w.buf, int64(w.bytesBuf.Len()))
	_, err = w.bufWriter.Write(w.buf[:size])
	if err != nil {
		return err
	}
	_, err = w.bufWriter.Write(w.bytesBuf.Bytes())
	if err != nil {
		return err
	}

	// Write values.
	// TODO(xichen): Use a streaming encoder to directly encode values into an bufio.Writer
	// instead of writing it to in-memory byte buffer and then to file.
	var encoded []byte
	switch valueIt.valueType {
	case field.NullType:
		break
	case field.BoolType:
		w.bw.Reset()
		err = w.bw.Encode(valueIt.boolIt)
		encoded = w.bw.Bytes()
	case field.IntType:
		w.iw.Reset()
		err = w.iw.Encode(valueIt.intIt)
		encoded = w.iw.Bytes()
	case field.DoubleType:
		w.dw.Reset()
		err = w.dw.Encode(valueIt.doubleIt)
		encoded = w.dw.Bytes()
	case field.StringType:
		w.sw.Reset()
		err = w.sw.Encode(valueIt.stringIt)
		encoded = w.sw.Bytes()
	}
	if err != nil {
		return err
	}
	// NB: `ensureBufferSize` has been called above so no need to call again.
	size = binary.PutVarint(w.buf, int64(len(encoded)))
	_, err = w.bufWriter.Write(w.buf[:size])
	if err != nil {
		return err
	}
	_, err = w.bufWriter.Write(encoded)
	if err != nil {
		return err
	}

	// Flush.
	return w.bufWriter.Flush()
}

func (w *writer) writeCheckpointFile() error {
	path := checkpointFilePath(w.segmentDir)
	f, err := w.openWritable(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func (w *writer) openWritable(filePath string) (*os.File, error) {
	return openWritable(filePath, w.newFileMode)
}

func (w *writer) ensureBufferSize(targetSize int) {
	w.buf = xbytes.EnsureBufferSize(w.buf, targetSize, xbytes.DontCopyData)
}

type valueIteratorUnion struct {
	valueType field.ValueType
	boolIt    boolenc.Iterator
	intIt     intenc.Iterator
	doubleIt  doubleenc.Iterator
	stringIt  stringenc.Iterator
}
