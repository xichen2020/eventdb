package fs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/xichen2020/eventdb/digest"
	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/generated/proto/infopb"
	"github.com/xichen2020/eventdb/persist/schema"
	xbytes "github.com/xichen2020/eventdb/x/bytes"

	"github.com/pilosa/pilosa/roaring"
)

// segmentWriter is responsible for writing segments to filesystem.
// TODO(xichen): Make docIDs an interface with `WriteTo()` to abstract the details
// of how the doc IDs are encoded.
// TODO(xichen): Encapsulate type-specific write functions for writing compound files.
type segmentWriter interface {
	// Open opens the writer.
	Open(opts writerOpenOptions) error

	// WriteTimestamps writes event timestamps.
	WriteTimestamps(timeNanos []int64) error

	// WriteRawDocs writes raw documents.
	WriteRawDocs(docs []string) error

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
	SegmentID    string
	MinTimeNanos int64
	MaxTimeNanos int64
	NumDocuments int32
}

type writer struct {
	filePathPrefix     string
	newFileMode        os.FileMode
	newDirectoryMode   os.FileMode
	fieldPathSeparator string
	rawDocSourceField  string
	timestampField     string

	fdWithDigestWriter digest.FdWithDigestWriter
	info               *infopb.SegmentInfo
	segmentDir         string
	numDocuments       int32
	buf                []byte
	bytesBuf           bytes.Buffer

	bw encoding.BoolEncoder
	iw encoding.IntEncoder
	dw encoding.DoubleEncoder
	sw encoding.StringEncoder
	tw encoding.TimeEncoder

	boolIt   encoding.RewindableBoolIterator
	intIt    encoding.RewindableIntIterator
	doubleIt encoding.RewindableDoubleIterator
	stringIt encoding.RewindableStringIterator
	timeIt   encoding.RewindableTimeIterator
	valueIt  valueIteratorUnion

	err error
}

// newSegmentWriter creates a new segment writer.
// TODO(xichen): Initialize the type-specific encoders.
// TODO(xichen): Encode timestamp with configurable precision.
// TODO(xichen): Validate the raw doc source field does not conflict with existing field paths.
// TODO(xichen): Add encoding hints when encoding raw docs.
// TODO(xichen): Investigate the benefit of writing a single field file.
func newSegmentWriter(opts *Options) segmentWriter {
	w := &writer{
		filePathPrefix:     opts.FilePathPrefix(),
		newFileMode:        opts.NewFileMode(),
		newDirectoryMode:   opts.NewDirectoryMode(),
		fieldPathSeparator: string(opts.FieldPathSeparator()),
		rawDocSourceField:  opts.RawDocSourceField(),
		timestampField:     opts.TimestampField(),
		fdWithDigestWriter: digest.NewFdWithDigestWriter(opts.WriteBufferSize()),
		info:               &infopb.SegmentInfo{},
		boolIt:             encoding.NewArrayBasedBoolIterator(nil),
		intIt:              encoding.NewArrayBasedIntIterator(nil),
		doubleIt:           encoding.NewArrayBasedDoubleIterator(nil),
		stringIt:           encoding.NewArrayBasedStringIterator(nil),
	}
	w.valueIt = valueIteratorUnion{
		boolIt:   w.boolIt,
		intIt:    w.intIt,
		doubleIt: w.doubleIt,
		stringIt: w.stringIt,
		timeIt:   w.timeIt,
	}
	return w
}

func (w *writer) Open(opts writerOpenOptions) error {
	var (
		namespace    = opts.Namespace
		shard        = opts.Shard
		minTimeNanos = opts.MinTimeNanos
		maxTimeNanos = opts.MaxTimeNanos
	)

	shardDir := shardDataDirPath(w.filePathPrefix, namespace, shard)
	segmentDir := segmentDirPath(shardDir, minTimeNanos, maxTimeNanos, opts.SegmentID)
	if err := os.MkdirAll(segmentDir, w.newDirectoryMode); err != nil {
		return err
	}
	w.segmentDir = segmentDir
	w.numDocuments = opts.NumDocuments
	w.err = nil

	w.info.Reset()
	w.info.Version = schema.SegmentVersion
	w.info.MinTimestampNanos = opts.MinTimeNanos
	w.info.MaxTimestampNanos = opts.MaxTimeNanos
	w.info.NumDocuments = opts.NumDocuments
	return w.writeInfoFile(segmentDir, w.info)
}

func (w *writer) WriteTimestamps(timeNanos []int64) error {
	var fieldPath [1]string
	fieldPath[0] = w.timestampField
	docIDSet := docIDSetUnion{
		docIDSetType: schema.FullDocIDSet,
		numDocs:      w.numDocuments,
	}
	w.timeIt.Reset(timeNanos)
	w.valueIt.valueType = field.TimeType
	return w.writeFieldDataFileInternal(w.segmentDir, fieldPath[:], docIDSet, w.valueIt)
}

func (w *writer) WriteRawDocs(docs []string) error {
	var fieldPath [1]string
	fieldPath[0] = w.rawDocSourceField
	docIDSet := docIDSetUnion{
		docIDSetType: schema.FullDocIDSet,
		numDocs:      w.numDocuments,
	}
	w.stringIt.Reset(docs)
	w.valueIt.valueType = field.StringType
	return w.writeFieldDataFileInternal(w.segmentDir, fieldPath[:], docIDSet, w.valueIt)
}

func (w *writer) WriteNullField(fieldPath []string, docIDs *roaring.Bitmap) error {
	w.valueIt.valueType = field.NullType
	return w.writeFieldDataFile(w.segmentDir, fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteBoolField(fieldPath []string, docIDs *roaring.Bitmap, vals []bool) error {
	w.boolIt.Reset(vals)
	w.valueIt.valueType = field.BoolType
	return w.writeFieldDataFile(w.segmentDir, fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteIntField(fieldPath []string, docIDs *roaring.Bitmap, vals []int) error {
	w.intIt.Reset(vals)
	w.valueIt.valueType = field.IntType
	return w.writeFieldDataFile(w.segmentDir, fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteDoubleField(fieldPath []string, docIDs *roaring.Bitmap, vals []float64) error {
	w.doubleIt.Reset(vals)
	w.valueIt.valueType = field.DoubleType
	return w.writeFieldDataFile(w.segmentDir, fieldPath, docIDs, w.valueIt)
}

func (w *writer) WriteStringField(fieldPath []string, docIDs *roaring.Bitmap, vals []string) error {
	w.stringIt.Reset(vals)
	w.valueIt.valueType = field.StringType
	return w.writeFieldDataFile(w.segmentDir, fieldPath, docIDs, w.valueIt)
}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}
	// NB(xichen): only write out the checkpoint file if there are no errors
	// encountered between calling writer.Open() and writer.Close().
	if err := w.writeCheckpointFile(w.segmentDir); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) writeInfoFile(
	segmentDir string,
	info *infopb.SegmentInfo,
) error {
	path := infoFilePath(segmentDir)
	f, err := w.openWritable(path)
	if err != nil {
		return err
	}
	w.fdWithDigestWriter.Reset(f)
	defer w.fdWithDigestWriter.Close()

	msgSize := info.Size()
	payloadSize := maxMessageSizeInBytes + msgSize
	w.ensureBufferSize(payloadSize)
	size := binary.PutVarint(w.buf, int64(msgSize))
	n, err := info.MarshalTo(w.buf[size:])
	if err != nil {
		return err
	}
	size += n
	_, err = w.fdWithDigestWriter.Write(w.buf[:size])
	if err != nil {
		return err
	}
	return w.fdWithDigestWriter.Flush()
}

func (w *writer) writeFieldDataFile(
	segmentDir string,
	fieldPath []string,
	docIDs *roaring.Bitmap,
	valueIt valueIteratorUnion,
) error {
	docIDSetType := schema.PartialDocIDSet
	if int32(docIDs.Count()) == w.numDocuments {
		docIDSetType = schema.FullDocIDSet
	}
	docIDSet := docIDSetUnion{
		docIDSetType: docIDSetType,
		numDocs:      w.numDocuments,
		docIDs:       docIDs,
	}
	w.err = w.writeFieldDataFileInternal(segmentDir, fieldPath, docIDSet, valueIt)
	return w.err
}

func (w *writer) writeFieldDataFileInternal(
	segmentDir string,
	fieldPath []string,
	docIDSet docIDSetUnion,
	valueIt valueIteratorUnion,
) error {
	if w.err != nil {
		return w.err
	}
	path := fieldDataFilePath(segmentDir, fieldPath, w.fieldPathSeparator, &w.bytesBuf)
	f, err := w.openWritable(path)
	if err != nil {
		return err
	}
	w.fdWithDigestWriter.Reset(f)
	defer w.fdWithDigestWriter.Close()

	// Write header.
	_, err = w.fdWithDigestWriter.Write(magicHeader)
	if err != nil {
		return err
	}
	if err = w.writeDocIDSet(w.fdWithDigestWriter, docIDSet); err != nil {
		return err
	}
	if err = w.writeValues(w.fdWithDigestWriter, valueIt); err != nil {
		return err
	}

	return w.fdWithDigestWriter.Flush()
}

func (w *writer) writeDocIDSet(
	writer digest.FdWithDigestWriter,
	docIDSet docIDSetUnion,
) error {
	// Encode the doc ID set type.
	w.ensureBufferSize(1)
	w.buf[0] = byte(docIDSet.docIDSetType)
	_, err := writer.Write(w.buf[:1])
	if err != nil {
		return err
	}

	// Encode the doc IDs.
	switch docIDSet.docIDSetType {
	case schema.PartialDocIDSet:
		if docIDSet.docIDs.Count() == 0 {
			return errEmptyDocIDSet
		}
		return w.writePartialDocIDSet(writer, docIDSet.docIDs)
	case schema.FullDocIDSet:
		return w.writeFullDocIDSet(writer, docIDSet.numDocs)
	default:
		return fmt.Errorf("unknown doc ID set type: %v", docIDSet.docIDSetType)
	}
}

func (w *writer) writePartialDocIDSet(
	writer digest.FdWithDigestWriter,
	docIDs *roaring.Bitmap,
) error {
	// TODO(xichen): Precompute the size of the encoded bitmap to avoid the memory
	// cost of writing the encoded bitmap to a byte buffer then to file.
	w.bytesBuf.Reset()
	_, err := docIDs.WriteTo(&w.bytesBuf)
	if err != nil {
		return err
	}
	w.ensureBufferSize(maxMessageSizeInBytes)
	size := binary.PutVarint(w.buf, int64(w.bytesBuf.Len()))
	_, err = writer.Write(w.buf[:size])
	if err != nil {
		return err
	}
	_, err = writer.Write(w.bytesBuf.Bytes())
	return err
}

func (w *writer) writeFullDocIDSet(
	writer digest.FdWithDigestWriter,
	numDocs int32,
) error {
	w.ensureBufferSize(binary.MaxVarintLen32)
	size := binary.PutVarint(w.buf, int64(numDocs))
	_, err := writer.Write(w.buf[:size])
	return err
}

func (w *writer) writeValues(
	writer digest.FdWithDigestWriter,
	valueIt valueIteratorUnion,
) error {
	switch valueIt.valueType {
	case field.NullType:
		return nil
	case field.BoolType:
		w.bw.Reset()
		return w.bw.Encode(writer, valueIt.boolIt)
	case field.IntType:
		w.iw.Reset()
		return w.iw.Encode(writer, valueIt.intIt)
	case field.DoubleType:
		w.dw.Reset()
		return w.dw.Encode(writer, valueIt.doubleIt)
	case field.StringType:
		w.sw.Reset()
		return w.sw.Encode(writer, valueIt.stringIt)
	case field.TimeType:
		w.tw.Reset()
		return w.tw.Encode(writer, valueIt.timeIt)
	default:
		return fmt.Errorf("unknown value type: %v", valueIt.valueType)
	}
}

func (w *writer) writeCheckpointFile(segmentDir string) error {
	path := checkpointFilePath(segmentDir)
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

type docIDSetUnion struct {
	docIDSetType schema.DocIDSetType
	numDocs      int32
	docIDs       *roaring.Bitmap
}

type valueIteratorUnion struct {
	valueType field.ValueType
	boolIt    encoding.RewindableBoolIterator
	intIt     encoding.RewindableIntIterator
	doubleIt  encoding.RewindableDoubleIterator
	stringIt  encoding.RewindableStringIterator
	timeIt    encoding.RewindableTimeIterator
}
