package fs

import (
	"errors"
	"sync"

	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/x/hash"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/uber-go/tally"
)

var (
	errEmptyFieldListToRetrieve = errors.New("empty field list to retrieve")
)

type readersByShard map[uint32]readersBySegment
type readersBySegment map[persist.SegmentMetadata]segmentReader

type fieldRetrieverMetrics struct {
	retrieveField  instrument.MethodMetrics
	retrieveFields instrument.MethodMetrics
}

func newFieldRetrieverMetrics(
	scope tally.Scope,
	samplingRate float64,
) fieldRetrieverMetrics {
	return fieldRetrieverMetrics{
		retrieveField:  instrument.NewMethodMetrics(scope, "retrieve-field", samplingRate),
		retrieveFields: instrument.NewMethodMetrics(scope, "retrieve-fields", samplingRate),
	}
}

// fieldRetriever is responsible for retrieving segment fields from filesystem.
// It handles the field retrieval across namespaces and shards, which facilitates
// caching (e.g., if a field is being retrieved then it doesn't need to be retrieved
// again) as well as coordinating the retrieval operations across multiple retrievals
// (e.g., rate limiting on how many concurrent disk reads are performed).
type fieldRetriever struct {
	sync.RWMutex

	opts *Options

	readersByNamespace map[hash.Hash]readersByShard
	nowFn              clock.NowFn

	metrics fieldRetrieverMetrics
}

// NewFieldRetriever creates a new field retriever.
func NewFieldRetriever(opts *Options) persist.FieldRetriever {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	samplingRate := instrumentOpts.MetricsSamplingRate()
	return &fieldRetriever{
		opts:               opts,
		nowFn:              opts.ClockOptions().NowFn(),
		readersByNamespace: make(map[hash.Hash]readersByShard),
		metrics:            newFieldRetrieverMetrics(scope, samplingRate),
	}
}

func (r *fieldRetriever) RetrieveField(
	namespace []byte,
	shard uint32,
	segmentMeta persist.SegmentMetadata,
	field persist.RetrieveFieldOptions,
) (indexfield.DocsField, error) {
	callStart := r.nowFn()
	reader, err := r.getReaderOrInsert(namespace, shard, segmentMeta)
	if err != nil {
		r.metrics.retrieveField.ReportError(r.nowFn().Sub(callStart))
		return nil, err
	}
	docsField, err := reader.ReadField(field)
	r.metrics.retrieveField.ReportSuccessOrError(err, r.nowFn().Sub(callStart))
	return docsField, err
}

func (r *fieldRetriever) RetrieveFields(
	namespace []byte,
	shard uint32,
	segmentMeta persist.SegmentMetadata,
	fields []persist.RetrieveFieldOptions,
) ([]indexfield.DocsField, error) {
	callStart := r.nowFn()
	if len(fields) == 0 {
		r.metrics.retrieveFields.ReportError(r.nowFn().Sub(callStart))
		return nil, errEmptyFieldListToRetrieve
	}
	reader, err := r.getReaderOrInsert(namespace, shard, segmentMeta)
	if err != nil {
		r.metrics.retrieveFields.ReportError(r.nowFn().Sub(callStart))
		return nil, err
	}
	res := make([]indexfield.DocsField, len(fields))
	for i, fieldMeta := range fields {
		field, err := reader.ReadField(fieldMeta)
		if err != nil {
			// Close the result fields that have been read so far.
			for j := 0; j < i; j++ {
				if res[j] != nil {
					res[j].Close()
					res[j] = nil
				}
			}
			r.metrics.retrieveFields.ReportError(r.nowFn().Sub(callStart))
			return nil, err
		}
		res[i] = field
	}
	r.metrics.retrieveFields.ReportSuccess(r.nowFn().Sub(callStart))
	return res, nil
}

func (r *fieldRetriever) getReaderOrInsert(
	namespace []byte,
	shard uint32,
	segmentMeta persist.SegmentMetadata,
) (segmentReader, error) {
	var (
		namespaceHash = hash.BytesHash(namespace)
		reader        segmentReader
	)
	r.RLock()
	shardedReaders, nsExists := r.readersByNamespace[namespaceHash]
	if nsExists {
		segmentReaders, segmentExists := shardedReaders[shard]
		if segmentExists {
			reader = segmentReaders[segmentMeta]
		}
	}
	r.RUnlock()

	if reader != nil {
		return reader, nil
	}

	r.Lock()
	defer r.Unlock()

	shardedReaders, nsExists = r.readersByNamespace[namespaceHash]
	if !nsExists {
		shardedReaders = make(readersByShard)
		r.readersByNamespace[namespaceHash] = shardedReaders
	}
	segmentReaders, segmentExists := shardedReaders[shard]
	if !segmentExists {
		segmentReaders = make(readersBySegment)
		shardedReaders[shard] = segmentReaders
	}
	reader, exists := segmentReaders[segmentMeta]
	if exists {
		return reader, nil
	}

	reader = newSegmentReader(namespace, shard, r.opts)
	if err := reader.Open(readerOpenOptions{
		SegmentMeta: segmentMeta,
	}); err != nil {
		return nil, err
	}
	segmentReaders[segmentMeta] = reader
	return reader, nil
}
