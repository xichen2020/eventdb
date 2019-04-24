package storage

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	skiplist "github.com/notbdu/fast-skiplist"
	"github.com/xichen2020/eventdb/document"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/x/bytes"

	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xlog "github.com/m3db/m3/src/x/log"
	"github.com/uber-go/tally"
)

// databaseNamespace is a database namespace.
type databaseNamespace interface {
	// ID returns the ID of the namespace.
	ID() []byte

	// Write writes a batch of documents within the namespace.
	WriteBatch(
		ctx context.Context,
		docs []document.Document,
	) error

	// QueryRaw performs a raw query against the documents in the namespace.
	QueryRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
	) (*query.RawResults, error)

	// QueryGrouped performs a group query against the documents in the namespace.
	QueryGrouped(
		ctx context.Context,
		q query.ParsedGroupedQuery,
	) (*query.GroupedResults, error)

	// QueryTimeBucket performs a time bucket query against the documents in the namespace.
	QueryTimeBucket(
		ctx context.Context,
		q query.ParsedTimeBucketQuery,
	) (*query.TimeBucketResults, error)

	// Tick performs a tick against the namespace.
	Tick(ctx context.Context) error

	// Close closes the namespace.
	Close() error
}

const (
	defaultBatchPercent = 0.1
)

var (
	errNamespaceAlreadyClosed = errors.New("namespace already closed")
)

type databaseNamespaceMetrics struct {
	unloadSuccess tally.Counter
	unloadErrors  tally.Counter
	flush         instrument.MethodMetrics
	tick          instrument.MethodMetrics
}

func newDatabaseNamespaceMetrics(
	scope tally.Scope,
	samplingRate float64,
) databaseNamespaceMetrics {
	return databaseNamespaceMetrics{
		unloadSuccess: scope.Counter("unload-success"),
		unloadErrors:  scope.Counter("unload-errors"),
		flush:         instrument.NewMethodMetrics(scope, "flush", samplingRate),
		tick:          instrument.NewMethodMetrics(scope, "tick", samplingRate),
	}
}

type newSegmentBuilderFn func(builderOpts *segment.BuilderOptions) segment.Builder

type dbNamespace struct {
	sync.RWMutex

	id                 []byte
	opts               *Options
	nsOpts             *NamespaceOptions
	segmentBuilderOpts *segment.BuilderOptions
	segmentOpts        *segmentOptions
	flushManager       databaseFlushManager
	logger             xlog.Logger

	closed bool

	// List of sealed segments sorted by maximum segment timestamp in ascending order.
	// This list is used for locating eligible segments during query time. Ideally it
	// can also auto rebalance after a sequence of insertion and deletions due to new
	// sealed segments becoming available and old segments expiring.
	sealedByMaxTimeAsc *skiplist.SkipList

	metrics             databaseNamespaceMetrics
	nowFn               clock.NowFn
	newSegmentBuilderFn newSegmentBuilderFn
}

func newDatabaseNamespace(
	nsMeta NamespaceMetadata,
	opts *Options,
) *dbNamespace {
	idClone := make([]byte, len(nsMeta.ID()))
	copy(idClone, nsMeta.ID())

	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	samplingRate := instrumentOpts.MetricsSamplingRate()

	fieldBuilderOpts := indexfield.NewDocsFieldBuilderOptions().
		SetBoolArrayPool(opts.BoolArrayPool()).
		SetIntArrayPool(opts.IntArrayPool()).
		SetDoubleArrayPool(opts.DoubleArrayPool()).
		SetBytesArrayPool(opts.BytesArrayPool()).
		SetInt64ArrayPool(opts.Int64ArrayPool()).
		// Reset string array to avoid holding onto documents after we've returned the referencing
		// array to the memory pool.
		SetBytesArrayResetFn(bytes.ResetArray)
	segmentBuilderOpts := segment.NewBuilderOptions().
		SetFieldBuilderOptions(fieldBuilderOpts).
		SetTimestampFieldPath(opts.TimestampFieldPath()).
		SetRawDocSourceFieldPath(opts.RawDocSourceFieldPath()).
		SetFieldHashFn(opts.FieldHashFn())

	segmentOpts := &segmentOptions{
		nowFn:                opts.ClockOptions().NowFn(),
		unloadAfterUnreadFor: opts.SegmentUnloadAfterUnreadFor(),
		instrumentOptions:    instrumentOpts,
		fieldRetriever:       opts.FieldRetriever(),
		executor:             opts.QueryExecutor(),
	}

	return &dbNamespace{
		id:                  idClone,
		opts:                opts,
		nsOpts:              nsMeta.Options(),
		segmentBuilderOpts:  segmentBuilderOpts,
		segmentOpts:         segmentOpts,
		flushManager:        opts.databaseFlushManager(),
		logger:              opts.InstrumentOptions().Logger(),
		sealedByMaxTimeAsc:  skiplist.New(),
		metrics:             newDatabaseNamespaceMetrics(scope, samplingRate),
		nowFn:               opts.ClockOptions().NowFn(),
		newSegmentBuilderFn: segment.NewBuilder,
	}
}

func (n *dbNamespace) ID() []byte { return n.id }

func (n *dbNamespace) WriteBatch(
	ctx context.Context,
	docs []document.Document,
) error {
	if len(docs) == 0 {
		return nil
	}
	builder := n.newSegmentBuilderFn(n.segmentBuilderOpts)
	builder.AddBatch(docs)
	rawSegment := builder.Build()
	payload := &segmentPayload{
		namespace: n.id,
		seg:       rawSegment,
	}
	payload.wg.Add(1)

	// Enqueue the segment.
	n.RLock()
	if n.closed {
		n.RUnlock()
		rawSegment.Close()
		return errNamespaceAlreadyClosed
	}
	err := n.flushManager.Enqueue(payload)
	n.RUnlock()

	if err != nil {
		rawSegment.Close()
		return err
	}

	// Wait for flush to finish if needed.
	callbackFn := func() error {
		payload.wg.Wait()
		if payload.resultErr != nil {
			rawSegment.Close()
			return payload.resultErr
		}

		// On flush success, clear in-memory data (but keep the metadata) and update the list
		// of searchable segments.
		rawSegment.ClearAll()

		n.Lock()
		defer n.Unlock()

		if n.closed {
			rawSegment.Close()
			return errNamespaceAlreadyClosed
		}
		newSegment := newDatabaseSegment(n.id, rawSegment, n.segmentOpts)
		n.sealedByMaxTimeAsc.Set(float64(rawSegment.Metadata().MaxTimeNanos), newSegment)
		return nil
	}

	if n.opts.WriteAsync() {
		go callbackFn()
		return nil
	}

	return callbackFn()
}

// NB(xichen): Can optimize by accessing sealed segments first if the query requires
// sorting by @timestamp in ascending order, and possibly terminate early without
// accessing the active segment.
func (n *dbNamespace) QueryRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
) (*query.RawResults, error) {
	retentionStartNanos := n.nowFn().Add(-n.nsOpts.Retention()).UnixNano()
	if q.StartNanosInclusive < retentionStartNanos {
		q.StartNanosInclusive = retentionStartNanos
	}

	n.RLock()
	if n.closed {
		n.RUnlock()
		return nil, errNamespaceAlreadyClosed
	}
	segments := n.segmentsForWithRLock(q.StartNanosInclusive, q.EndNanosExclusive)
	n.RUnlock()

	res := q.NewRawResults()
	for _, segment := range segments {
		if err := segment.QueryRaw(ctx, q, res); err != nil {
			return nil, err
		}
		if res.IsComplete() {
			break
		}
	}
	return res, nil
}

func (n *dbNamespace) QueryGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
) (*query.GroupedResults, error) {
	retentionStartNanos := n.nowFn().Add(-n.nsOpts.Retention()).UnixNano()
	if q.StartNanosInclusive < retentionStartNanos {
		q.StartNanosInclusive = retentionStartNanos
	}

	n.RLock()
	if n.closed {
		n.RUnlock()
		return nil, errNamespaceAlreadyClosed
	}
	segments := n.segmentsForWithRLock(q.StartNanosInclusive, q.EndNanosExclusive)
	n.RUnlock()

	res := q.NewGroupedResults()
	for _, segment := range segments {
		if err := segment.QueryGrouped(ctx, q, res); err != nil {
			return nil, err
		}
		if res.IsComplete() {
			break
		}
	}
	return res, nil
}

func (n *dbNamespace) QueryTimeBucket(
	ctx context.Context,
	q query.ParsedTimeBucketQuery,
) (*query.TimeBucketResults, error) {
	retentionStartNanos := n.nowFn().Add(-n.nsOpts.Retention()).UnixNano()
	if q.StartNanosInclusive < retentionStartNanos {
		q.StartNanosInclusive = retentionStartNanos
	}

	n.RLock()
	if n.closed {
		n.RUnlock()
		return nil, errNamespaceAlreadyClosed
	}
	segments := n.segmentsForWithRLock(q.StartNanosInclusive, q.EndNanosExclusive)
	n.RUnlock()

	res := q.NewTimeBucketResults()
	for _, segment := range segments {
		if err := segment.QueryTimeBucket(ctx, q, res); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// TODO(xichen): Need to expire and discard segments that have expired, which requires
// the skiplist to implement a method to delete all elements before and including
// a given element. Ignore for now due to lack of proper API.
// TODO(xichen): Propagate ticking stats back up.
func (n *dbNamespace) Tick(ctx context.Context) error {
	callStart := n.nowFn()
	err := n.tryUnloadSegments(ctx)
	n.metrics.tick.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return err
}

func (n *dbNamespace) Close() error {
	n.Lock()
	defer n.Unlock()

	if n.closed {
		return errNamespaceAlreadyClosed
	}
	n.closed = true
	byMaxTimeAsc := n.sealedByMaxTimeAsc
	n.sealedByMaxTimeAsc = nil

	if byMaxTimeAsc.Length == 0 {
		return nil
	}
	for elem := byMaxTimeAsc.Front(); elem != nil; elem = elem.Next() {
		segment := elem.Value().(*dbSegment)
		segment.Close()
	}
	return nil
}

func (n *dbNamespace) segmentsForWithRLock(
	startNanosInclusive int64,
	endNanosExclusive int64,
) []*dbSegment {
	var segments []*dbSegment
	// Find the first element whose max timestamp is no later than the start time of the query range.
	geElem := n.sealedByMaxTimeAsc.GetGreaterThanOrEqualTo(float64(startNanosInclusive))
	for elem := geElem; elem != nil; elem = elem.Next() {
		segment := elem.Value().(*dbSegment)
		meta := segment.Metadata()
		if meta.MinTimeNanos >= endNanosExclusive {
			continue
		}
		segments = append(segments, segment)
	}
	return segments
}

// nolint: unparam
func (n *dbNamespace) tryUnloadSegments(ctx context.Context) error {
	var toUnload []*dbSegment
	err := n.forEachSegment(func(segment *dbSegment) error {
		if segment.ShouldUnload() {
			toUnload = append(toUnload, segment)
		}
		return nil
	})
	if err != nil {
		return err
	}

	var multiErr xerrors.MultiError
	for _, segment := range toUnload {
		// If we get here, it means the segment should be unloaded, and as such
		// it means there are no current readers reading from the segment (otherwise
		// `ShouldUnload` will return false).
		if err := segment.Unload(); err != nil {
			multiErr = multiErr.Add(err)
			n.metrics.unloadErrors.Inc(1)
		} else {
			n.metrics.unloadSuccess.Inc(1)
		}
	}

	return multiErr.FinalError()
}

type segmentFn func(segment *dbSegment) error

// NB(xichen): This assumes that a skiplist element may not become invalid (e.g., deleted)
// after new elements are inserted into the skiplist while iterating over a batch of elements.
// A brief look at the skiplist implementation seems to suggest this is the case but may need
// a closer look to validate fully.
func (n *dbNamespace) forEachSegment(segmentFn segmentFn) error {
	// Determine batch size.
	n.RLock()
	if n.closed {
		n.RUnlock()
		return errNamespaceAlreadyClosed
	}
	elemsLen := n.sealedByMaxTimeAsc.Length
	if elemsLen == 0 {
		// If the list is empty, nothing to do.
		n.RUnlock()
		return nil
	}
	batchSize := int(math.Max(1.0, math.Ceil(defaultBatchPercent*float64(elemsLen))))
	currElem := n.sealedByMaxTimeAsc.Front()
	n.RUnlock()

	var multiErr xerrors.MultiError
	currSegments := make([]*dbSegment, 0, batchSize)
	for currElem != nil {
		n.RLock()
		if n.closed {
			n.RUnlock()
			return errNamespaceAlreadyClosed
		}
		for numChecked := 0; numChecked < batchSize && currElem != nil; numChecked++ {
			nextElem := currElem.Next()
			ss := currElem.Value().(*dbSegment)
			currSegments = append(currSegments, ss)
			currElem = nextElem
		}
		n.RUnlock()

		for _, segment := range currSegments {
			if err := segmentFn(segment); err != nil {
				multiErr = multiErr.Add(err)
			}
		}
		for i := range currSegments {
			currSegments[i] = nil
		}
		currSegments = currSegments[:0]
	}
	return multiErr.FinalError()
}

// NamespaceMetadata provides namespace-level metadata.
type NamespaceMetadata struct {
	id   []byte
	opts *NamespaceOptions
}

// NewNamespaceMetadata creates a new namespace metadata.
func NewNamespaceMetadata(id []byte, opts *NamespaceOptions) NamespaceMetadata {
	if opts == nil {
		opts = NewNamespaceOptions()
	}
	return NamespaceMetadata{id: id, opts: opts}
}

// ID returns the namespace ID.
func (m NamespaceMetadata) ID() []byte { return m.id }

// Options return the namespace options.
func (m NamespaceMetadata) Options() *NamespaceOptions { return m.opts }

// NamespaceOptions provide a set of options controlling namespace-level behavior.
type NamespaceOptions struct {
	retention time.Duration
}

const (
	defaultNamespaceRetention = 24 * time.Hour
)

// NewNamespaceOptions create a new set of namespace options.
func NewNamespaceOptions() *NamespaceOptions {
	return &NamespaceOptions{
		retention: defaultNamespaceRetention,
	}
}

// SetRetention sets the namespace retention.
func (o *NamespaceOptions) SetRetention(v time.Duration) *NamespaceOptions {
	opts := *o
	opts.retention = v
	return &opts
}

// Retention returns the namespce retention.
func (o *NamespaceOptions) Retention() time.Duration {
	return o.retention
}
