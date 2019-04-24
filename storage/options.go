package storage

import (
	"os"
	"time"

	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/persist/fs"
	"github.com/xichen2020/eventdb/query/executor"
	"github.com/xichen2020/eventdb/x/hash"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	defaultFieldPathSeparator          = '.'
	defaultNamespaceFieldName          = "service"
	defaultTickMinInterval             = time.Minute
	defaultSegmentUnloadAfterUnreadFor = 5 * time.Minute
	defaultFlushQueueSize              = 1024
	defaultWriteAsync                  = false
)

var (
	defaultTimestampFieldPath    = []string{"@timestamp"}
	defaultRawDocSourceFieldPath = []string{"_source"}
	defaultPersistManager        = fs.NewPersistManager(nil)
	defaultFilePathPrefix        = os.TempDir()
)

// Options provide a set of options for the database.
type Options struct {
	clockOpts                   clock.Options
	instrumentOpts              instrument.Options
	filePathPrefix              string
	fieldPathSeparator          byte
	fieldHashFn                 hash.StringArrayHashFn
	namespaceFieldName          string
	timeStampFieldPath          []string
	rawDocSourceFieldPath       []string
	persistManager              persist.Manager
	tickMinInterval             time.Duration
	segmentUnloadAfterUnreadFor time.Duration
	fieldRetriever              persist.FieldRetriever
	queryExecutor               executor.Executor
	writeAsync                  bool
	flushQueueSize              int
	flushManager                databaseFlushManager
	boolArrayPool               *pool.BucketizedBoolArrayPool
	intArrayPool                *pool.BucketizedIntArrayPool
	int64ArrayPool              *pool.BucketizedInt64ArrayPool
	doubleArrayPool             *pool.BucketizedFloat64ArrayPool
	bytesArrayPool              *pool.BucketizedBytesArrayPool
}

// NewOptions create a new set of options.
func NewOptions() *Options {
	o := &Options{
		clockOpts:                   clock.NewOptions(),
		instrumentOpts:              instrument.NewOptions(),
		filePathPrefix:              defaultFilePathPrefix,
		fieldPathSeparator:          defaultFieldPathSeparator,
		fieldHashFn:                 defaultFieldHashFn,
		namespaceFieldName:          defaultNamespaceFieldName,
		timeStampFieldPath:          defaultTimestampFieldPath,
		rawDocSourceFieldPath:       defaultRawDocSourceFieldPath,
		persistManager:              defaultPersistManager,
		tickMinInterval:             defaultTickMinInterval,
		writeAsync:                  defaultWriteAsync,
		flushQueueSize:              defaultFlushQueueSize,
		segmentUnloadAfterUnreadFor: defaultSegmentUnloadAfterUnreadFor,
		queryExecutor:               executor.NewExecutor(),
	}
	o.initPools()
	return o
}

// SetClockOptions sets the clock options.
func (o *Options) SetClockOptions(v clock.Options) *Options {
	opts := *o
	opts.clockOpts = v
	return &opts
}

// ClockOptions returns the clock options.
func (o *Options) ClockOptions() clock.Options {
	return o.clockOpts
}

// SetInstrumentOptions sets the instrument options.
func (o *Options) SetInstrumentOptions(v instrument.Options) *Options {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *Options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetFilePathPrefix sets the file path prefix.
func (o *Options) SetFilePathPrefix(v string) *Options {
	opts := *o
	opts.filePathPrefix = v
	return &opts
}

// FilePathPrefix return the file path prefix.
func (o *Options) FilePathPrefix() string {
	return o.filePathPrefix
}

// SetPersistManager sets the persistence manager.
func (o *Options) SetPersistManager(v persist.Manager) *Options {
	opts := *o
	opts.persistManager = v
	return &opts
}

// PersistManager returns the persistence manager.
func (o *Options) PersistManager() persist.Manager {
	return o.persistManager
}

// SetFieldPathSeparator sets the path separator when flattening nested document fields.
// This is used when persisting and querying nested fields.
func (o *Options) SetFieldPathSeparator(v byte) *Options {
	opts := *o
	opts.fieldPathSeparator = v
	return &opts
}

// FieldPathSeparator returns the path separator when flattening nested document fields.
// This is used when persisting and querying nested fields.
func (o *Options) FieldPathSeparator() byte {
	return o.fieldPathSeparator
}

// SetFieldHashFn sets the field hashing function.
func (o *Options) SetFieldHashFn(v hash.StringArrayHashFn) *Options {
	opts := *o
	opts.fieldHashFn = v
	return &opts
}

// FieldHashFn returns the field hashing function.
func (o *Options) FieldHashFn() hash.StringArrayHashFn {
	return o.fieldHashFn
}

// SetNamespaceFieldName sets the field name of the namespace field.
func (o *Options) SetNamespaceFieldName(v string) *Options {
	opts := *o
	opts.namespaceFieldName = v
	return &opts
}

// NamespaceFieldName returns the field name of the namespace field.
func (o *Options) NamespaceFieldName() string {
	return o.namespaceFieldName
}

// SetTimestampFieldPath sets the timestamp field path.
func (o *Options) SetTimestampFieldPath(v []string) *Options {
	opts := *o
	opts.timeStampFieldPath = v
	return &opts
}

// TimestampFieldPath returns the timestamp field path.
func (o *Options) TimestampFieldPath() []string {
	return o.timeStampFieldPath
}

// SetRawDocSourceFieldPath sets the raw document source field path.
func (o *Options) SetRawDocSourceFieldPath(v []string) *Options {
	opts := *o
	opts.rawDocSourceFieldPath = v
	return &opts
}

// RawDocSourceFieldPath returns the raw document source field path.
func (o *Options) RawDocSourceFieldPath() []string {
	return o.rawDocSourceFieldPath
}

// SetTickMinInterval sets the minimum interval between consecutive ticks
// for performing periodic administrative tasks (e.g., unloading segments
// from memory) to smoothen the load.
func (o *Options) SetTickMinInterval(v time.Duration) *Options {
	opts := *o
	opts.tickMinInterval = v
	return &opts
}

// TickMinInterval returns the minimum interval between consecutive ticks
// for performing periodic administrative tasks (e.g., unloading segments
// from memory) to smoothen the load.
func (o *Options) TickMinInterval() time.Duration {
	return o.tickMinInterval
}

// SetSegmentUnloadAfterUnreadFor sets the segment unload after unread for duration.
// If a segment is unread for longer than the configuration duration since its
// last read access, it is eligible to be unloaded from memory.
func (o *Options) SetSegmentUnloadAfterUnreadFor(v time.Duration) *Options {
	opts := *o
	opts.segmentUnloadAfterUnreadFor = v
	return &opts
}

// SegmentUnloadAfterUnreadFor sets the segment unload after unread for duration.
// If a segment is unread for longer than the configuration duration since its
// last read access, it is eligible to be unloaded from memory.
func (o *Options) SegmentUnloadAfterUnreadFor() time.Duration {
	return o.segmentUnloadAfterUnreadFor
}

// SetWriteAsync determines whether a new batch of incoming writes are performed asynchronously.
// If set to true, writes are acknowledged when they are enqueued to the flush queue.
// Otherwise, writes are acknowledged when they are flushed to disk.
func (o *Options) SetWriteAsync(v bool) *Options {
	opts := *o
	opts.writeAsync = v
	return &opts
}

// WriteAsync determines whether a new batch of incoming writes are performed asynchronously.
// If set to true, writes are acknowledged when they are enqueued to the flush queue.
// Otherwise, writes are acknowledged when they are flushed to disk.
func (o *Options) WriteAsync() bool {
	return o.writeAsync
}

// SetFlushQueueSize sets the flush queue size.
func (o *Options) SetFlushQueueSize(v int) *Options {
	opts := *o
	opts.flushQueueSize = v
	return &opts
}

// FlushQueueSize returns the flush queue size.
func (o *Options) FlushQueueSize() int {
	return o.flushQueueSize
}

// SetFieldRetriever sets the field retriever.
func (o *Options) SetFieldRetriever(v persist.FieldRetriever) *Options {
	opts := *o
	opts.fieldRetriever = v
	return &opts
}

// FieldRetriever returns the field retriever.
func (o *Options) FieldRetriever() persist.FieldRetriever {
	return o.fieldRetriever
}

// SetQueryExecutor sets the query executor.
func (o *Options) SetQueryExecutor(v executor.Executor) *Options {
	opts := *o
	opts.queryExecutor = v
	return &opts
}

// QueryExecutor returns the query executor.
func (o *Options) QueryExecutor() executor.Executor {
	return o.queryExecutor
}

func (o *Options) setDatabaseFlushManager(v databaseFlushManager) *Options {
	opts := *o
	opts.flushManager = v
	return &opts
}

func (o *Options) databaseFlushManager() databaseFlushManager {
	return o.flushManager
}

// SetBoolArrayPool sets the bool array pool.
func (o *Options) SetBoolArrayPool(v *pool.BucketizedBoolArrayPool) *Options {
	opts := *o
	opts.boolArrayPool = v
	return &opts
}

// BoolArrayPool returns the bool array pool.
func (o *Options) BoolArrayPool() *pool.BucketizedBoolArrayPool {
	return o.boolArrayPool
}

// SetIntArrayPool sets the int array pool.
func (o *Options) SetIntArrayPool(v *pool.BucketizedIntArrayPool) *Options {
	opts := *o
	opts.intArrayPool = v
	return &opts
}

// IntArrayPool returns the int array pool.
func (o *Options) IntArrayPool() *pool.BucketizedIntArrayPool {
	return o.intArrayPool
}

// SetInt64ArrayPool sets the int64 array pool.
func (o *Options) SetInt64ArrayPool(v *pool.BucketizedInt64ArrayPool) *Options {
	opts := *o
	opts.int64ArrayPool = v
	return &opts
}

// Int64ArrayPool returns the int64 array pool.
func (o *Options) Int64ArrayPool() *pool.BucketizedInt64ArrayPool {
	return o.int64ArrayPool
}

// SetDoubleArrayPool sets the double array pool.
func (o *Options) SetDoubleArrayPool(v *pool.BucketizedFloat64ArrayPool) *Options {
	opts := *o
	opts.doubleArrayPool = v
	return &opts
}

// DoubleArrayPool returns the double array pool.
func (o *Options) DoubleArrayPool() *pool.BucketizedFloat64ArrayPool {
	return o.doubleArrayPool
}

// SetBytesArrayPool sets the string array pool.
func (o *Options) SetBytesArrayPool(v *pool.BucketizedBytesArrayPool) *Options {
	opts := *o
	opts.bytesArrayPool = v
	return &opts
}

// BytesArrayPool returns the string array pool.
func (o *Options) BytesArrayPool() *pool.BucketizedBytesArrayPool {
	return o.bytesArrayPool
}

func (o *Options) initPools() {
	boolArrayPool := pool.NewBucketizedBoolArrayPool(nil, nil)
	boolArrayPool.Init(func(capacity int) []bool { return make([]bool, 0, capacity) })
	o.boolArrayPool = boolArrayPool

	intArrayPool := pool.NewBucketizedIntArrayPool(nil, nil)
	intArrayPool.Init(func(capacity int) []int { return make([]int, 0, capacity) })
	o.intArrayPool = intArrayPool

	int64ArrayPool := pool.NewBucketizedInt64ArrayPool(nil, nil)
	int64ArrayPool.Init(func(capacity int) []int64 { return make([]int64, 0, capacity) })
	o.int64ArrayPool = int64ArrayPool

	doubleArrayPool := pool.NewBucketizedFloat64ArrayPool(nil, nil)
	doubleArrayPool.Init(func(capacity int) []float64 { return make([]float64, 0, capacity) })
	o.doubleArrayPool = doubleArrayPool

	bytesArrayPool := pool.NewBucketizedBytesArrayPool(nil, nil)
	bytesArrayPool.Init(func(capacity int) [][]byte { return make([][]byte, 0, capacity) })
	o.bytesArrayPool = bytesArrayPool
}

func defaultFieldHashFn(fieldPath []string) hash.Hash {
	return hash.StringArrayHash(fieldPath, defaultFieldPathSeparator)
}
