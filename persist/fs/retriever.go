package fs

import (
	"errors"
	"sync"

	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/x/hash"
)

var (
	errEmptyFieldListToRetrieve = errors.New("empty field list to retrieve")
)

type readersByShard map[uint32]readersBySegment
type readersBySegment map[persist.SegmentMetadata]segmentReader

// fieldRetriever is responsible for retrieving segment fields from filesystem.
// It handles the field retrieval across namespaces and shards, which facilitates
// caching (e.g., if a field is being retrieved then it doesn't need to be retrieved
// again) as well as coordinating the retrieval operations across multiple retrievals
// (e.g., rate limiting on how many concurrent disk reads are performed).
type fieldRetriever struct {
	sync.RWMutex

	opts *Options

	readersByNamespace map[hash.Hash]readersByShard
}

// NewFieldRetriever creates a new field retriever.
func NewFieldRetriever(opts *Options) persist.FieldRetriever {
	return &fieldRetriever{
		opts:               opts,
		readersByNamespace: make(map[hash.Hash]readersByShard),
	}
}

func (r *fieldRetriever) RetrieveField(
	namespace []byte,
	shard uint32,
	segmentMeta persist.SegmentMetadata,
	field persist.RetrieveFieldOptions,
) (index.DocsField, error) {
	reader, err := r.getReaderOrInsert(namespace, shard, segmentMeta)
	if err != nil {
		return nil, err
	}
	return reader.ReadField(field)
}

func (r *fieldRetriever) RetrieveFields(
	namespace []byte,
	shard uint32,
	segmentMeta persist.SegmentMetadata,
	fields []persist.RetrieveFieldOptions,
) ([]index.DocsField, error) {
	if len(fields) == 0 {
		return nil, errEmptyFieldListToRetrieve
	}
	reader, err := r.getReaderOrInsert(namespace, shard, segmentMeta)
	if err != nil {
		return nil, err
	}
	res := make([]index.DocsField, len(fields))
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
			return nil, err
		}
		res[i] = field
	}
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
