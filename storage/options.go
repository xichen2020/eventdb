package storage

import (
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/persist/fs"
	"github.com/xichen2020/eventdb/x/pool"
)

const (
	defaultFieldPathSeparator           = '.'
	defaultNamespaceFieldName           = "service"
	defaultTimestampFieldName           = "@timestamp"
	defaultMaxNumCachedSegmentsPerShard = 1
)

var (
	defaultPersistManager = fs.NewPersistManager(nil)
)

// Options provide a set of options for the database.
type Options struct {
	clockOpts                    clock.Options
	instrumentOpts               instrument.Options
	fieldPathSeparator           byte
	namespaceFieldName           string
	timeStampFieldName           string
	persistManager               persist.Manager
	maxNumCachedSegmentsPerShard int
	boolArrayPool                *pool.BucketizedBoolArrayPool
	intArrayPool                 *pool.BucketizedIntArrayPool
	int64ArrayPool               *pool.BucketizedInt64ArrayPool
	doubleArrayPool              *pool.BucketizedFloat64ArrayPool
	stringArrayPool              *pool.BucketizedStringArrayPool
}

// NewOptions create a new set of options.
func NewOptions() *Options {
	o := &Options{
		clockOpts:                    clock.NewOptions(),
		instrumentOpts:               instrument.NewOptions(),
		fieldPathSeparator:           defaultFieldPathSeparator,
		namespaceFieldName:           defaultNamespaceFieldName,
		timeStampFieldName:           defaultTimestampFieldName,
		persistManager:               defaultPersistManager,
		maxNumCachedSegmentsPerShard: defaultMaxNumCachedSegmentsPerShard,
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

// SetFieldPathSeparator sets the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) SetFieldPathSeparator(v byte) *Options {
	opts := *o
	opts.fieldPathSeparator = v
	return &opts
}

// FieldPathSeparator returns the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) FieldPathSeparator() byte {
	return o.fieldPathSeparator
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

// SetTimestampFieldName sets the field name of the timestamp field.
func (o *Options) SetTimestampFieldName(v string) *Options {
	opts := *o
	opts.timeStampFieldName = v
	return &opts
}

// TimestampFieldName returns the field name of the timestamp field.
func (o *Options) TimestampFieldName() string {
	return o.timeStampFieldName
}

// SetMaxNumCachedSegmentsPerShard sets the maximum number of segments cached in
// memory per shard in a namespace.
func (o *Options) SetMaxNumCachedSegmentsPerShard(v int) *Options {
	opts := *o
	opts.maxNumCachedSegmentsPerShard = v
	return &opts
}

// MaxNumCachedSegmentsPerShard returns the maximum number of segments cached in
// memory per shard in a namespace.
func (o *Options) MaxNumCachedSegmentsPerShard() int {
	return o.maxNumCachedSegmentsPerShard
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

// SetStringArrayPool sets the string array pool.
func (o *Options) SetStringArrayPool(v *pool.BucketizedStringArrayPool) *Options {
	opts := *o
	opts.stringArrayPool = v
	return &opts
}

// StringArrayPool returns the string array pool.
func (o *Options) StringArrayPool() *pool.BucketizedStringArrayPool {
	return o.stringArrayPool
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

	stringArrayPool := pool.NewBucketizedStringArrayPool(nil, nil)
	stringArrayPool.Init(func(capacity int) []string { return make([]string, 0, capacity) })
	o.stringArrayPool = stringArrayPool
}
