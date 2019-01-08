package fs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/xichen2020/eventdb/digest"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/infopb"
	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/values/decoding"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/mmap"

	xlog "github.com/m3db/m3x/log"
)

// segmentReader is responsible for reading segments from filesystem.
type segmentReader interface {
	// Open opens the reader.
	Open(opts readerOpenOptions) error

	// ReadField reads a single document field for given field metadata.
	ReadField(fieldMeta persist.RetrieveFieldOptions) (index.DocsField, error)

	// Close closes the reader.
	Close() error
}

var (
	errSegmentReaderClosed    = errors.New("segment reader is closed")
	errCheckpointFileNotFound = errors.New("checkpoint file not found")
	errMagicHeaderMismatch    = errors.New("magic header mismatch")
)

// readerOpenOptions provide a set of options for reading fields.
type readerOpenOptions struct {
	SegmentMeta persist.SegmentMetadata
}

// TODO(xichen): Roundtrip tests.
type reader struct {
	namespace          []byte
	shard              uint32
	filePathPrefix     string
	fieldPathSeparator string
	shardDir           string
	timestampPrecision time.Duration
	mmapHugeTLBOpts    mmap.HugeTLBOptions
	logger             xlog.Logger

	info              *infopb.SegmentInfo
	bytesBuf          bytes.Buffer
	segmentDir        string
	minTimestampNanos int64
	maxTimestampNanos int64
	numDocuments      int32

	closed bool
	bd     decoding.BoolDecoder
	id     decoding.IntDecoder
	dd     decoding.DoubleDecoder
	sd     decoding.StringDecoder
	td     decoding.TimeDecoder
}

// newSegmentReader creates a new segment reader.
func newSegmentReader(
	namespace []byte,
	shard uint32,
	opts *Options,
) segmentReader {
	r := &reader{
		namespace:          namespace,
		shard:              shard,
		filePathPrefix:     opts.FilePathPrefix(),
		fieldPathSeparator: string(opts.FieldPathSeparator()),
		shardDir:           shardDataDirPath(opts.FilePathPrefix(), namespace, shard),
		timestampPrecision: opts.TimestampPrecision(),
		mmapHugeTLBOpts: mmap.HugeTLBOptions{
			Enabled:   opts.MmapEnableHugePages(),
			Threshold: opts.MmapHugePagesThreshold(),
		},
		logger: opts.InstrumentOptions().Logger(),
		info:   &infopb.SegmentInfo{},
		bd:     decoding.NewBoolDecoder(),
		id:     decoding.NewIntDecoder(),
		dd:     decoding.NewDoubleDecoder(),
		sd:     decoding.NewStringDecoder(),
		td:     decoding.NewTimeDecoder(),
	}
	return r
}

func (r *reader) Open(opts readerOpenOptions) error {
	if r.closed {
		return errSegmentReaderClosed
	}
	r.segmentDir = segmentDirPath(r.shardDir, opts.SegmentMeta)

	// Check if the checkpoint file exists, and bail early if not.
	if err := r.readCheckpointFile(r.segmentDir); err != nil {
		return err
	}

	// Read the info file.
	if err := r.readInfoFile(r.segmentDir); err != nil {
		return err
	}

	return nil
}

func (r *reader) ReadField(fieldMeta persist.RetrieveFieldOptions) (index.DocsField, error) {
	if r.closed {
		return nil, errSegmentReaderClosed
	}

	var (
		fieldPath  = fieldMeta.FieldPath
		fieldTypes = make([]field.ValueType, 0, len(fieldMeta.FieldTypes))
		nf         index.CloseableNullField
		bf         index.CloseableBoolField
		intf       index.CloseableIntField
		df         index.CloseableDoubleField
		sf         index.CloseableStringField
		tf         index.CloseableTimeField
		err        error
	)

	for t := range fieldMeta.FieldTypes {
		switch t {
		case field.NullType:
			nf, err = r.readNullField(fieldPath)
		case field.BoolType:
			bf, err = r.readBoolField(fieldPath)
		case field.IntType:
			intf, err = r.readIntField(fieldPath)
		case field.DoubleType:
			df, err = r.readDoubleField(fieldPath)
		case field.StringType:
			sf, err = r.readStringField(fieldPath)
		case field.TimeType:
			tf, err = r.readTimeField(fieldPath)
		default:
			err = fmt.Errorf("unknown field type %v", t)
		}
		if err != nil {
			break
		}
		fieldTypes = append(fieldTypes, t)
	}

	if err == nil {
		res := index.NewDocsField(fieldPath, fieldTypes, nf, bf, intf, df, sf, tf)
		return res, nil
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

	return nil, err
}

func (r *reader) Close() error {
	if r.closed {
		return errSegmentReaderClosed
	}
	r.closed = true
	r.info = nil
	r.bd = nil
	r.id = nil
	r.dd = nil
	r.sd = nil
	r.td = nil
	return nil
}

func (r *reader) readCheckpointFile(segmentDir string) error {
	path := checkpointFilePath(segmentDir)
	exists, err := fileExists(path)
	if err != nil {
		return err
	}
	if !exists {
		return errCheckpointFileNotFound
	}
	return nil
}

func (r *reader) readInfoFile(segmentDir string) error {
	path := infoFilePath(segmentDir)
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()

	data, err := r.mmapReadAllAndValidateChecksum(fd)
	if err != nil {
		return err
	}
	defer mmap.Munmap(data)

	size, bytesRead, err := io.ReadVarint(data)
	if err != nil {
		return err
	}
	r.info.Reset()
	if err := r.info.Unmarshal(data[bytesRead : bytesRead+int(size)]); err != nil {
		return err
	}
	r.minTimestampNanos = r.info.MinTimestampNanos
	r.maxTimestampNanos = r.info.MaxTimestampNanos
	r.numDocuments = r.info.NumDocuments

	return nil
}

func (r *reader) readNullField(fieldPath []string) (index.CloseableNullField, error) {
	rawData, cleanup, err := r.readAndValidateFieldData(fieldPath)
	if err != nil {
		return nil, err
	}

	docIDSet, _, err := r.readDocIDSet(rawData)
	if err != nil {
		cleanup()
		return nil, err
	}

	return index.NewCloseableNullFieldWithCloseFn(docIDSet, cleanup), nil
}

func (r *reader) readBoolField(fieldPath []string) (index.CloseableBoolField, error) {
	rawData, cleanup, err := r.readAndValidateFieldData(fieldPath)
	if err != nil {
		return nil, err
	}

	docIDSet, remainder, err := r.readDocIDSet(rawData)
	if err != nil {
		cleanup()
		return nil, err
	}

	values, err := r.bd.DecodeRaw(remainder)
	if err != nil {
		cleanup()
		return nil, err
	}
	return index.NewCloseableBoolFieldWithCloseFn(docIDSet, values, cleanup), nil
}

func (r *reader) readIntField(fieldPath []string) (index.CloseableIntField, error) {
	rawData, cleanup, err := r.readAndValidateFieldData(fieldPath)
	if err != nil {
		return nil, err
	}

	docIDSet, remainder, err := r.readDocIDSet(rawData)
	if err != nil {
		cleanup()
		return nil, err
	}

	values, err := r.id.DecodeRaw(remainder)
	if err != nil {
		cleanup()
		return nil, err
	}
	return index.NewCloseableIntFieldWithCloseFn(docIDSet, values, cleanup), nil
}

func (r *reader) readDoubleField(fieldPath []string) (index.CloseableDoubleField, error) {
	rawData, cleanup, err := r.readAndValidateFieldData(fieldPath)
	if err != nil {
		return nil, err
	}

	docIDSet, remainder, err := r.readDocIDSet(rawData)
	if err != nil {
		cleanup()
		return nil, err
	}

	values, err := r.dd.DecodeRaw(remainder)
	if err != nil {
		cleanup()
		return nil, err
	}
	return index.NewCloseableDoubleFieldWithCloseFn(docIDSet, values, cleanup), nil
}

func (r *reader) readStringField(fieldPath []string) (index.CloseableStringField, error) {
	rawData, cleanup, err := r.readAndValidateFieldData(fieldPath)
	if err != nil {
		return nil, err
	}

	docIDSet, remainder, err := r.readDocIDSet(rawData)
	if err != nil {
		cleanup()
		return nil, err
	}

	values, err := r.sd.DecodeRaw(remainder)
	if err != nil {
		cleanup()
		return nil, err
	}
	return index.NewCloseableStringFieldWithCloseFn(docIDSet, values, cleanup), nil
}

func (r *reader) readTimeField(fieldPath []string) (index.CloseableTimeField, error) {
	rawData, cleanup, err := r.readAndValidateFieldData(fieldPath)
	if err != nil {
		return nil, err
	}

	docIDSet, remainder, err := r.readDocIDSet(rawData)
	if err != nil {
		cleanup()
		return nil, err
	}

	values, err := r.td.DecodeRaw(remainder)
	if err != nil {
		cleanup()
		return nil, err
	}
	return index.NewCloseableTimeFieldWithCloseFn(docIDSet, values, cleanup), nil
}

func (r *reader) readAndValidateFieldData(fieldPath []string) ([]byte, func(), error) {
	filePath := fieldDataFilePath(r.segmentDir, fieldPath, r.fieldPathSeparator, &r.bytesBuf)
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	data, err := r.mmapReadAllAndValidateChecksum(fd)
	if err != nil {
		fd.Close()
		return nil, nil, err
	}

	cleanup := func() {
		fd.Close()
		mmap.Munmap(data)
	}

	// Validate magic header.
	if len(data) < len(magicHeader) || !bytes.Equal(data[:len(magicHeader)], magicHeader) {
		cleanup()
		return nil, nil, errMagicHeaderMismatch
	}
	return data[len(magicHeader):], cleanup, nil
}

// readAllAndValidate reads all the data from the given file via mmap and validates
// the contents against its checksum. If the validation passes, it returns the mmaped
// bytes. Otherwise, an error is returned.
func (r *reader) mmapReadAllAndValidateChecksum(fd *os.File) ([]byte, error) {
	res, err := mmap.File(fd, mmap.Options{Read: true, HugeTLB: r.mmapHugeTLBOpts})
	if err != nil {
		return nil, err
	}
	if res.Warning != nil {
		r.logger.Warnf("warning during memory mapping info file %s: %s", fd.Name(), res.Warning)
	}
	if err := digest.Validate(res.Result); err != nil {
		mmap.Munmap(res.Result)
		return nil, err
	}
	return res.Result, nil
}

func (r *reader) readDocIDSet(data []byte) (index.DocIDSet, []byte, error) {
	docIDSet, bytesRead, err := index.NewDocIDSetFromBytes(data)
	if err != nil {
		return nil, nil, err
	}
	return docIDSet, data[bytesRead:], nil
}
