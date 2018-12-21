package fs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/xichen2020/eventdb/digest"
	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/generated/proto/infopb"
	"github.com/xichen2020/eventdb/persist/schema"
	xbytes "github.com/xichen2020/eventdb/x/bytes"
)

// segmentWriter is responsible for writing segments to filesystem.
// TODO(xichen): Make docIDs an interface with `WriteTo()` to abstract the details
// of how the doc IDs are encoded.
type segmentWriter interface {
	// Open opens the writer.
	Open(opts writerOpenOptions) error

	// WriteFields writes a set of document fields.
	WriteFields(fields []document.DocsField) error

	// Close closes the writer.
	Close() error
}

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
	timestampField     string
	timestampPrecision time.Duration
	rawDocSourceField  string

	fdWithDigestWriter digest.FdWithDigestWriter
	info               *infopb.SegmentInfo
	segmentDir         string
	numDocuments       int32
	buf                []byte
	bytesBuf           bytes.Buffer

	bw      encoding.BoolEncoder
	iw      encoding.IntEncoder
	dw      encoding.DoubleEncoder
	sw      encoding.StringEncoder
	tw      encoding.TimeEncoder
	valueIt valueIteratorUnion

	err error
}

// newSegmentWriter creates a new segment writer.
// TODO(xichen): Initialize the type-specific encoders and allow encoding timestamp with precision.
// TODO(xichen): Add encoding hints when encoding raw docs.
// TODO(xichen): Validate the raw doc source field does not conflict with existing field paths.
// TODO(xichen): Investigate the benefit of writing a single field file.
func newSegmentWriter(opts *Options) segmentWriter {
	w := &writer{
		filePathPrefix:     opts.FilePathPrefix(),
		newFileMode:        opts.NewFileMode(),
		newDirectoryMode:   opts.NewDirectoryMode(),
		fieldPathSeparator: string(opts.FieldPathSeparator()),
		timestampField:     opts.TimestampField(),
		timestampPrecision: opts.TimestampPrecision(),
		rawDocSourceField:  opts.RawDocSourceField(),
		fdWithDigestWriter: digest.NewFdWithDigestWriter(opts.WriteBufferSize()),
		info:               &infopb.SegmentInfo{},
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

func (w *writer) WriteFields(fields []document.DocsField) error {
	for _, field := range fields {
		if err := w.writeField(field); err != nil {
			return err
		}
	}
	return nil
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

func (w *writer) writeField(df document.DocsField) error {
	path := df.FieldPath()

	// Write null values.
	if docIDSet, exists := df.NullIter(); exists {
		w.valueIt.valueType = field.NullType
		if err := w.writeFieldDataFile(w.segmentDir, path, docIDSet, w.valueIt); err != nil {
			return err
		}
	}

	// Write boolean values.
	if docIDSet, boolIt, exists := df.BoolIter(); exists {
		w.valueIt.valueType = field.BoolType
		w.valueIt.boolIt = boolIt
		if err := w.writeFieldDataFile(w.segmentDir, path, docIDSet, w.valueIt); err != nil {
			return err
		}
	}

	// Write int values.
	if docIDSet, intIt, exists := df.IntIter(); exists {
		w.valueIt.valueType = field.IntType
		w.valueIt.intIt = intIt
		if err := w.writeFieldDataFile(w.segmentDir, path, docIDSet, w.valueIt); err != nil {
			return err
		}
	}

	// Write double values.
	if docIDSet, doubleIt, exists := df.DoubleIter(); exists {
		w.valueIt.valueType = field.DoubleType
		w.valueIt.doubleIt = doubleIt
		if err := w.writeFieldDataFile(w.segmentDir, path, docIDSet, w.valueIt); err != nil {
			return err
		}
	}

	// Write string values.
	if docIDSet, stringIt, exists := df.StringIter(); exists {
		w.valueIt.valueType = field.StringType
		w.valueIt.stringIt = stringIt
		if err := w.writeFieldDataFile(w.segmentDir, path, docIDSet, w.valueIt); err != nil {
			return err
		}
	}

	// Write time values.
	if docIDSet, timeIt, exists := df.TimeIter(); exists {
		w.valueIt.valueType = field.TimeType
		w.valueIt.timeIt = timeIt
		if err := w.writeFieldDataFile(w.segmentDir, path, docIDSet, w.valueIt); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) writeFieldDataFile(
	segmentDir string,
	fieldPath []string,
	docIDSet document.DocIDSet,
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
	docIDSet document.DocIDSet,
) error {
	docIDSetType := schema.PartialDocIDSet
	if docIDSet.IsFull() {
		docIDSetType = schema.FullDocIDSet
	}

	// Encode the doc ID set type.
	w.ensureBufferSize(1)
	w.buf[0] = byte(docIDSetType)
	_, err := writer.Write(w.buf[:1])
	if err != nil {
		return err
	}

	return docIDSet.WriteTo(writer, &w.bytesBuf)
}

func (w *writer) writeValues(
	writer digest.FdWithDigestWriter,
	valueIt valueIteratorUnion,
) error {
	switch valueIt.valueType {
	case field.NullType:
		return nil
	case field.BoolType:
		return w.bw.Encode(writer, valueIt.boolIt)
	case field.IntType:
		return w.iw.Encode(writer, valueIt.intIt)
	case field.DoubleType:
		return w.dw.Encode(writer, valueIt.doubleIt)
	case field.StringType:
		return w.sw.Encode(writer, valueIt.stringIt)
	case field.TimeType:
		// TODO(bodu): have the resolution bubble down from the storage options config.
		return w.tw.Encode(writer, valueIt.timeIt, encoding.EncodeTimeOptions{Resolution: time.Nanosecond})
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

type valueIteratorUnion struct {
	valueType field.ValueType
	boolIt    encoding.RewindableBoolIterator
	intIt     encoding.RewindableIntIterator
	doubleIt  encoding.RewindableDoubleIterator
	stringIt  encoding.RewindableStringIterator
	timeIt    encoding.RewindableTimeIterator
}
