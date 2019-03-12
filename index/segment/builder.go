package segment

import (
	"math"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/x/hash"
	"github.com/xichen2020/eventdb/x/safe"

	"github.com/pborman/uuid"
)

const (
	defaultInitialNumFields = 2048
)

// Builder builds a segment.
type Builder interface {
	// AddBatch adds a batch of documents to the segment.
	AddBatch(docs document.Documents)

	// Build returns a segment built from data accumulated in the builder.
	// The builder should not be used after `Build` is called.
	Build() MutableSegment
}

// FieldPathPredicate is a predicate on the given field path.
type FieldPathPredicate func(fieldPath []string) bool

// BuilderOptions provide a set of builder options.
type BuilderOptions struct {
	fieldBuilderOpts      *indexfield.DocsFieldBuilderOptions
	timestampFieldPath    []string
	rawDocSourceFieldPath []string
	fieldHashFn           hash.StringArrayHashFn
}

// NewBuilderOptions create a new set of builder options.
func NewBuilderOptions() *BuilderOptions {
	return &BuilderOptions{}
}

// SetFieldBuilderOptions sets the field builder options.
func (o *BuilderOptions) SetFieldBuilderOptions(v *indexfield.DocsFieldBuilderOptions) *BuilderOptions {
	opts := *o
	opts.fieldBuilderOpts = v
	return &opts
}

// FieldBuilderOptions sets the field builder options.
func (o *BuilderOptions) FieldBuilderOptions() *indexfield.DocsFieldBuilderOptions {
	return o.fieldBuilderOpts
}

// SetTimestampFieldPath sets the timestamp field path.
func (o *BuilderOptions) SetTimestampFieldPath(v []string) *BuilderOptions {
	opts := *o
	opts.timestampFieldPath = v
	return &opts
}

// TimestampFieldPath returns the timestamp field path.
func (o *BuilderOptions) TimestampFieldPath() []string {
	return o.timestampFieldPath
}

// SetRawDocSourceFieldPath sets the raw doc source field path.
func (o *BuilderOptions) SetRawDocSourceFieldPath(v []string) *BuilderOptions {
	opts := *o
	opts.rawDocSourceFieldPath = v
	return &opts
}

// RawDocSourceFieldPath returns the raw doc source field path.
func (o *BuilderOptions) RawDocSourceFieldPath() []string {
	return o.rawDocSourceFieldPath
}

// SetFieldHashFn sets the function that computes the hash of a field path.
func (o *BuilderOptions) SetFieldHashFn(v hash.StringArrayHashFn) *BuilderOptions {
	opts := *o
	opts.fieldHashFn = v
	return &opts
}

// FieldHashFn returns the function that computes the hash of a field path.
func (o *BuilderOptions) FieldHashFn() hash.StringArrayHashFn {
	return o.fieldHashFn
}

// builder is not thread safe for performance reasons.
// Concurrent access must be protected externally.
type builder struct {
	fieldBuilderOpts *indexfield.DocsFieldBuilderOptions
	segmentOpts      *segmentOptions
	fieldHashFn      hash.StringArrayHashFn

	metadata Metadata
	fields   map[hash.Hash]indexfield.DocsFieldBuilder
	// These two builders provide fast access to builders for the timestamp field
	// and the raw doc source field which are present in every index.
	// TODO(xichen): Use full doc ID set builder for these two fields.
	timestampField    indexfield.DocsFieldBuilder
	rawDocSourceField indexfield.DocsFieldBuilder
}

// NewBuilder creates a new segment builder.
func NewBuilder(opts *BuilderOptions) Builder {
	var (
		fieldBuilderOpts         = opts.FieldBuilderOptions()
		fieldHashFn              = opts.FieldHashFn()
		timestampFieldPath       = opts.TimestampFieldPath()
		rawDocSourceFieldPath    = opts.RawDocSourceFieldPath()
		timestampFieldHash       = fieldHashFn(timestampFieldPath)
		rawDocSourceFieldHash    = fieldHashFn(rawDocSourceFieldPath)
		timestampFieldBuilder    = indexfield.NewDocsFieldBuilder(timestampFieldPath, fieldBuilderOpts)
		rawDocSourceFieldBuilder = indexfield.NewDocsFieldBuilder(rawDocSourceFieldPath, fieldBuilderOpts)
	)

	segmentOpts := &segmentOptions{
		timestampFieldPath:    timestampFieldPath,
		rawDocSourceFieldPath: rawDocSourceFieldPath,
		fieldHashFn:           fieldHashFn,
	}
	fields := make(map[hash.Hash]indexfield.DocsFieldBuilder, defaultInitialNumFields)
	fields[timestampFieldHash] = timestampFieldBuilder
	fields[rawDocSourceFieldHash] = rawDocSourceFieldBuilder

	initMeta := Metadata{
		ID:           uuid.NewUUID().String(),
		MinTimeNanos: math.MaxInt64,
		MaxTimeNanos: -1,
	}

	return &builder{
		fieldBuilderOpts:  opts.FieldBuilderOptions(),
		segmentOpts:       segmentOpts,
		fieldHashFn:       fieldHashFn,
		metadata:          initMeta,
		fields:            fields,
		timestampField:    timestampFieldBuilder,
		rawDocSourceField: rawDocSourceFieldBuilder,
	}
}

// AddBatch takes ownership of the data in the documents. They should not be reused or returned to pool.
func (b *builder) AddBatch(docs document.Documents) {
	for _, doc := range docs {
		docID := b.metadata.NumDocs
		b.metadata.NumDocs++
		if b.metadata.MinTimeNanos > doc.TimeNanos {
			b.metadata.MinTimeNanos = doc.TimeNanos
		}
		if b.metadata.MaxTimeNanos < doc.TimeNanos {
			b.metadata.MaxTimeNanos = doc.TimeNanos
		}
		b.writeTimestampField(docID, doc.TimeNanos)
		b.writeRawDocSourceField(docID, doc.RawData)
		for _, f := range doc.Fields {
			builder := b.getOrInsert(f.Path)
			builder.Add(docID, f.Value)
		}
	}
	b.metadata.NumFields = len(b.fields)
}

func (b *builder) Build() MutableSegment {
	segment := newSegmentWithBuilder(b.metadata, b.fields, b.segmentOpts)
	b.reset()
	return segment
}

func (b *builder) reset() {
	b.fieldBuilderOpts = nil
	b.segmentOpts = nil
	b.fieldHashFn = nil
	b.fields = nil
	b.timestampField = nil
	b.rawDocSourceField = nil
}

func (b *builder) writeTimestampField(docID int32, val int64) {
	v := field.ValueUnion{
		Type:         field.TimeType,
		TimeNanosVal: val,
	}
	b.timestampField.Add(docID, v)
}

func (b *builder) writeRawDocSourceField(docID int32, val []byte) {
	// TODO(xichen): Remove the conversion here once we finish replacing string with []byte.
	v := field.ValueUnion{
		Type:      field.StringType,
		StringVal: safe.ToString(val),
	}
	b.rawDocSourceField.Add(docID, v)
}

func (b *builder) getOrInsert(fieldPath []string) indexfield.DocsFieldBuilder {
	pathHash := b.fieldHashFn(fieldPath)
	if builder, exists := b.fields[pathHash]; exists {
		return builder
	}
	builder := indexfield.NewDocsFieldBuilder(fieldPath, b.fieldBuilderOpts)
	b.fields[pathHash] = builder
	return builder
}
