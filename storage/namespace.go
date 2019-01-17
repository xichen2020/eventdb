package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/sharding"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/uber-go/tally"
)

// databaseNamespace is a database namespace.
type databaseNamespace interface {
	// ID returns the ID of the namespace.
	ID() []byte

	// Write writes an document within the namespace.
	Write(doc document.Document) error

	// QueryRaw performs a raw query against the documents in the namespace.
	QueryRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
	) (query.RawResults, error)

	// QueryGrouped performs a group query against the documents in the namespace.
	QueryGrouped(
		ctx context.Context,
		q query.ParsedGroupedQuery,
	) (query.GroupedResults, error)

	// Tick performs a tick against the namespace.
	Tick(ctx context.Context) error

	// Flush performs a flush against the namespace.
	Flush(ps persist.Persister) error

	// Close closes the namespace.
	Close() error
}

var (
	errNamespaceAlreadyClosed = errors.New("namespace already closed")
)

type databaseNamespaceMetrics struct {
	flush instrument.MethodMetrics
	tick  instrument.MethodMetrics
}

func newDatabaseNamespaceMetrics(scope tally.Scope, samplingRate float64) databaseNamespaceMetrics {
	return databaseNamespaceMetrics{
		flush: instrument.NewMethodMetrics(scope, "flush", samplingRate),
		tick:  instrument.NewMethodMetrics(scope, "tick", samplingRate),
	}
}

type dbNamespace struct {
	sync.RWMutex

	id       []byte
	shardSet sharding.ShardSet
	opts     *Options
	nsOpts   *NamespaceOptions

	closed  bool
	shards  []databaseShard
	metrics databaseNamespaceMetrics
	nowFn   clock.NowFn
}

func newDatabaseNamespace(
	nsMeta NamespaceMetadata,
	shardSet sharding.ShardSet,
	opts *Options,
) *dbNamespace {
	idClone := make([]byte, len(nsMeta.ID()))
	copy(idClone, nsMeta.ID())

	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	samplingRate := instrumentOpts.MetricsSamplingRate()
	n := &dbNamespace{
		id:       idClone,
		shardSet: shardSet,
		opts:     opts,
		nsOpts:   nsMeta.Options(),
		metrics:  newDatabaseNamespaceMetrics(scope, samplingRate),
		nowFn:    opts.ClockOptions().NowFn(),
	}
	n.initShards()
	return n
}

func (n *dbNamespace) ID() []byte { return n.id }

func (n *dbNamespace) Write(doc document.Document) error {
	shard, err := n.shardFor(doc.ID)
	if err != nil {
		return err
	}
	return shard.Write(doc)
}

func (n *dbNamespace) QueryRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
) (query.RawResults, error) {
	retentionStartNanos := n.nowFn().Add(-n.nsOpts.Retention()).UnixNano()
	if q.StartNanosInclusive < retentionStartNanos {
		q.StartNanosInclusive = retentionStartNanos
	}

	res := q.NewRawResults()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		shardRes, err := shard.QueryRaw(ctx, q)
		if err != nil {
			return query.RawResults{}, err
		}
		res.AddBatch(shardRes.Data)
		if !res.IsOrdered() && res.LimitReached() {
			// We've got enough data, bail early.
			break
		}
	}
	return res, nil
}

func (n *dbNamespace) QueryGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
) (query.GroupedResults, error) {
	retentionStartNanos := n.nowFn().Add(-n.nsOpts.Retention()).UnixNano()
	if q.StartNanosInclusive < retentionStartNanos {
		q.StartNanosInclusive = retentionStartNanos
	}

	res := q.NewGroupedResults()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		shardRes, err := shard.QueryGrouped(ctx, q)
		if err != nil {
			return query.GroupedResults{}, err
		}
		res.AddBatch(shardRes.Groups)
		if !res.IsOrdered() && res.LimitReached() {
			// We've got enough data, bail early.
			break
		}
	}
	return res, nil
}

func (n *dbNamespace) Tick(ctx context.Context) error {
	callStart := n.nowFn()
	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		if err := shard.Tick(ctx); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	res := multiErr.FinalError()
	n.metrics.tick.ReportSuccessOrError(res, n.nowFn().Sub(callStart))
	return res
}

func (n *dbNamespace) Flush(ps persist.Persister) error {
	callStart := n.nowFn()
	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.Flush(ps); err != nil {
			detailedErr := fmt.Errorf("shard %d failed to flush data: %v", shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}

	res := multiErr.FinalError()
	n.metrics.flush.ReportSuccessOrError(res, n.nowFn().Sub(callStart))
	return res
}

func (n *dbNamespace) Close() error {
	n.Lock()
	if n.closed {
		n.Unlock()
		return errNamespaceAlreadyClosed
	}
	n.closed = true
	shards := n.shards
	n.shards = shards[:0]
	n.shardSet = sharding.NewEmptyShardSet(sharding.DefaultHashFn(1))
	n.Unlock()
	n.closeShards(shards)
	return nil
}

func (n *dbNamespace) initShards() {
	n.Lock()
	defer n.Unlock()

	shards := n.shardSet.AllIDs()
	dbShards := make([]databaseShard, n.shardSet.Max()+1)
	for _, shard := range shards {
		dbShards[shard] = newDatabaseShard(n.ID(), shard, n.opts, n.nsOpts)
	}
	n.shards = dbShards
}

func (n *dbNamespace) shardFor(id []byte) (databaseShard, error) {
	n.RLock()
	shardID := n.shardSet.Lookup(id)
	shard, err := n.shardAtWithRLock(shardID)
	n.RUnlock()
	return shard, err
}

func (n *dbNamespace) shardAtWithRLock(shardID uint32) (databaseShard, error) {
	if int(shardID) >= len(n.shards) {
		return nil, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	shard := n.shards[shardID]
	if shard == nil {
		return nil, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	return shard, nil
}

// nolint:megacheck
func (n *dbNamespace) getOwnedShards() []databaseShard {
	n.RLock()
	shards := n.shardSet.AllIDs()
	databaseShards := make([]databaseShard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = n.shards[shard]
	}
	n.RUnlock()
	return databaseShards
}

func (n *dbNamespace) closeShards(shards []databaseShard) {
	var wg sync.WaitGroup
	closeFn := func(shard databaseShard) {
		shard.Close()
		wg.Done()
	}

	wg.Add(len(shards))
	for _, shard := range shards {
		dbShard := shard
		if dbShard == nil {
			continue
		}
		go closeFn(dbShard)
	}

	wg.Wait()
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
