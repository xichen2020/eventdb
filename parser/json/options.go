package json

import "github.com/xichen2020/eventdb/value"

const (
	defaultMaxDepth = 3
)

// Options provide a set of parsing options.
type Options struct {
	maxDepth       int
	valuePool      *value.Pool
	valueArrayPool *value.BucketizedArrayPool
	kvArrayPool    *value.BucketizedKVArrayPool
}

// NewOptions creates a new set of parsing options.
func NewOptions() *Options {
	o := &Options{
		maxDepth:       defaultMaxDepth,
		valuePool:      value.NewPool(nil),
		valueArrayPool: value.NewBucketizedArrayPool(nil, nil),
		kvArrayPool:    value.NewBucketizedKVArrayPool(nil, nil),
	}
	o.initPools()
	return o
}

// SetMaxDepth sets the maximum depth eligible for parsing.
func (o *Options) SetMaxDepth(v int) *Options {
	opts := *o
	opts.maxDepth = v
	return &opts
}

// MaxDepth returns the maximum depth eligible for parsing.
func (o *Options) MaxDepth() int { return o.maxDepth }

// SetValuePool sets the pool for values.
func (o *Options) SetValuePool(v *value.Pool) *Options {
	opts := *o
	opts.valuePool = v
	return &opts
}

// ValuePool returns the pool for values.
func (o *Options) ValuePool() *value.Pool { return o.valuePool }

// SetValueArrayPool sets the pool for value arrays.
func (o *Options) SetValueArrayPool(v *value.BucketizedArrayPool) *Options {
	opts := *o
	opts.valueArrayPool = v
	return &opts
}

// ValueArrayPool returns the pool for value arrays.
func (o *Options) ValueArrayPool() *value.BucketizedArrayPool { return o.valueArrayPool }

// SetKVArrayPool sets the pool for KV arrays.
func (o *Options) SetKVArrayPool(v *value.BucketizedKVArrayPool) *Options {
	opts := *o
	opts.kvArrayPool = v
	return &opts
}

// KVArrayPool returns the pool for KV arrays.
func (o *Options) KVArrayPool() *value.BucketizedKVArrayPool { return o.kvArrayPool }

func (o *Options) initPools() {
	o.valuePool.Init(func() *value.Value { return value.NewEmptyValue(o.valuePool) })
	o.valueArrayPool.Init(func(capacity int) value.Array {
		values := make([]*value.Value, 0, capacity)
		return value.NewArray(values, o.valueArrayPool)
	})
	o.kvArrayPool.Init(func(capacity int) value.KVArray {
		kvs := make([]value.KV, 0, capacity)
		return value.NewKVArray(kvs, o.kvArrayPool)
	})
}
