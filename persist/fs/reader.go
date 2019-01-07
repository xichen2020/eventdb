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
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/mmap"

	xlog "github.com/m3db/m3x/log"
)

// SegmentReader is responsible for reading segments from filesystem.
type SegmentReader interface {
	// Open opens the reader.
	Open(opts ReaderOpenOptions) error

	// ReadFields reads a set of document fields for given field metadatas.
	ReadFields(fieldMetas ...index.DocsFieldMetadata) ([]index.DocsField, error)

	// Close closes the reader.
	Close() error
}

var (
	errCheckpointFileNotFound = errors.New("checkpoint file not found")
	errMagicHeaderMismatch    = errors.New("magic header mismatch")
)

// ReaderOpenOptions provide a set of options for reading fields.
type ReaderOpenOptions struct {
	Namespace      []byte
	Shard          uint32
	SegmentDirName string
}

type reader struct {
	filePathPrefix     string
	fieldPathSeparator string
	timestampPrecision time.Duration
	mmapHugeTLBOpts    mmap.HugeTLBOptions
	logger             xlog.Logger

	info              *infopb.SegmentInfo
	bytesBuf          bytes.Buffer
	segmentDir        string
	minTimestampNanos int64
	maxTimestampNanos int64
	numDocuments      int32

	err error
}

// NewSegmentReader creates a new segment reader.
func NewSegmentReader(opts *Options) SegmentReader {
	r := &reader{
		filePathPrefix:     opts.FilePathPrefix(),
		fieldPathSeparator: string(opts.FieldPathSeparator()),
		timestampPrecision: opts.TimestampPrecision(),
		mmapHugeTLBOpts: mmap.HugeTLBOptions{
			Enabled:   opts.MmapEnableHugePages(),
			Threshold: opts.MmapHugePagesThreshold(),
		},
		logger: opts.InstrumentOptions().Logger(),
		info:   &infopb.SegmentInfo{},
	}
	return r
}

func (r *reader) Open(opts ReaderOpenOptions) error {
	shardDir := shardDataDirPath(r.filePathPrefix, opts.Namespace, opts.Shard)
	segmentDir := segmentDirPathFromPrefixAndDirName(shardDir, opts.SegmentDirName)
	r.segmentDir = segmentDir
	r.err = nil

	// Check if the checkpoint file exists, and bail early if not.
	if err := r.readCheckpointFile(segmentDir); err != nil {
		return err
	}

	// Read the info file.
	if err := r.readInfoFile(segmentDir); err != nil {
		return err
	}

	return nil
}

func (r *reader) ReadFields(fieldMetas ...index.DocsFieldMetadata) ([]index.DocsField, error) {
	if len(fieldMetas) == 0 {
		return nil, nil
	}
	res := make([]index.DocsField, len(fieldMetas))
	for i, fieldMeta := range fieldMetas {
		field, err := r.readField(fieldMeta)
		if err != nil {
			// Close the result fields that have been read so far.
			for j := 0; j < i; j++ {
				if res[j] != nil {
					res[j].Close()
					res[j] = nil
				}
			}
			return nil, err
		}
		res[i] = field
	}
	return res, nil
}

func (r *reader) Close() error {
	return errors.New("not implemented")
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

func (r *reader) readField(fieldMeta index.DocsFieldMetadata) (index.DocsField, error) {
	var (
		fieldPath  = fieldMeta.FieldPath
		fieldTypes = fieldMeta.FieldTypes
		nf         index.CloseableNullField
		bf         index.CloseableBoolField
		intf       index.CloseableIntField
		df         index.CloseableDoubleField
		sf         index.CloseableStringField
		tf         index.CloseableTimeField
		err        error
	)

	for _, t := range fieldTypes {
		switch t {
		case field.NullType:
			nf, err = r.readNullField(fieldPath)
			if err != nil {
				break
			}
			if nf != nil {
				fieldTypes = append(fieldTypes, field.NullType)
			}
		case field.BoolType:
			bf, err = r.readBoolField(fieldPath)
			if err != nil {
				break
			}
			if bf != nil {
				fieldTypes = append(fieldTypes, field.BoolType)
			}
		case field.IntType:
			intf, err = r.readIntField(fieldPath)
			if err != nil {
				break
			}
			if intf != nil {
				fieldTypes = append(fieldTypes, field.IntType)
			}
		case field.DoubleType:
			df, err = r.readDoubleField(fieldPath)
			if err != nil {
				break
			}
			if df != nil {
				fieldTypes = append(fieldTypes, field.DoubleType)
			}
		case field.StringType:
			sf, err = r.readStringField(fieldPath)
			if err != nil {
				break
			}
			if sf != nil {
				fieldTypes = append(fieldTypes, field.StringType)
			}
		case field.TimeType:
			tf, err = r.readTimeField(fieldPath)
			if err != nil {
				break
			}
			if tf != nil {
				fieldTypes = append(fieldTypes, field.TimeType)
			}
		default:
			err = fmt.Errorf("unknown field type %v", t)
		}
		if err != nil {
			break
		}
	}

	if err == nil {
		// Make a copy of field types to avoid external mutation.
		typesClone := make([]field.ValueType, len(fieldTypes))
		copy(typesClone, fieldTypes)
		res := index.NewDocsField(fieldPath, typesClone, nf, bf, intf, df, sf, tf)
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

func (r *reader) readNullField(fieldPath []string) (index.CloseableNullField, error) {
	return nil, errors.New("not implemented")
}

func (r *reader) readBoolField(fieldPath []string) (index.CloseableBoolField, error) {
	return nil, errors.New("not implemented")
}

func (r *reader) readIntField(fieldPath []string) (index.CloseableIntField, error) {
	return nil, errors.New("not implemented")
}

func (r *reader) readDoubleField(fieldPath []string) (index.CloseableDoubleField, error) {
	return nil, errors.New("not implemented")
}

func (r *reader) readStringField(fieldPath []string) (index.CloseableStringField, error) {
	filePath := fieldDataFilePath(r.segmentDir, fieldPath, r.fieldPathSeparator, &r.bytesBuf)
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	data, err := r.mmapReadAllAndValidateChecksum(fd)
	if err != nil {
		fd.Close()
		return nil, err
	}

	cleanup := func() {
		fd.Close()
		mmap.Munmap(data)
	}

	// Validate magic header.
	if len(data) < len(magicHeader) || !bytes.Equal(data[:len(magicHeader)], magicHeader) {
		cleanup()
		return nil, errMagicHeaderMismatch
	}
	rawData := data[len(magicHeader):]

	docIDSet, remainder, err := r.readDocIDSet(rawData)
	if err != nil {
		cleanup()
		return nil, err
	}

	values, err := r.readStringValues(remainder)
	if err != nil {
		cleanup()
		return nil, err
	}
	return index.NewFsBasedStringField(docIDSet, values, cleanup), nil
}

func (r *reader) readTimeField(fieldPath []string) (index.CloseableTimeField, error) {
	return nil, errors.New("not implemented")
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

func (r *reader) readStringValues(data []byte) (values.CloseableStringValues, error) {
	return nil, errors.New("not implemented")
}
