// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package field

import (
	"fmt"

	"sort"

	"github.com/uber-go/tally"
)

// FieldArrayBucket specifies a bucket.
type FieldArrayBucket struct {
	// Capacity is the size of each element in the bucket.
	Capacity int

	// Count is the number of fixed elements in the bucket.
	Count int

	// Options is an optional override to specify options to use for a bucket,
	// specify nil to use the options specified to the bucketized pool
	// constructor for this bucket.
	Options *FieldArrayPoolOptions
}

// fieldArrayBucketByCapacity is a sortable collection of pool buckets.
type fieldArrayBucketByCapacity []FieldArrayBucket

func (x fieldArrayBucketByCapacity) Len() int {
	return len(x)
}

func (x fieldArrayBucketByCapacity) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x fieldArrayBucketByCapacity) Less(i, j int) bool {
	return x[i].Capacity < x[j].Capacity
}

type fieldArrayBucketPool struct {
	capacity int
	pool     *FieldArrayPool
}

// BucketizedFieldArrayPool is a bucketized value pool.
type BucketizedFieldArrayPool struct {
	sizesAsc          []FieldArrayBucket
	buckets           []fieldArrayBucketPool
	maxBucketCapacity int
	opts              *FieldArrayPoolOptions
	alloc             func(capacity int) []Field
	maxAlloc          tally.Counter
}

// NewBucketizedFieldArrayPool creates a bucketized object pool.
func NewBucketizedFieldArrayPool(sizes []FieldArrayBucket, opts *FieldArrayPoolOptions) *BucketizedFieldArrayPool {
	if opts == nil {
		opts = NewFieldArrayPoolOptions()
	}

	sizesAsc := make([]FieldArrayBucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(fieldArrayBucketByCapacity(sizesAsc))

	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}

	return &BucketizedFieldArrayPool{
		opts:              opts,
		sizesAsc:          sizesAsc,
		maxBucketCapacity: maxBucketCapacity,
		maxAlloc:          opts.InstrumentOptions().MetricsScope().Counter("alloc-max"),
	}
}

// Init initializes the bucketized pool.
func (p *BucketizedFieldArrayPool) Init(alloc func(capacity int) []Field) {
	buckets := make([]fieldArrayBucketPool, len(p.sizesAsc))
	for i := range p.sizesAsc {
		size := p.sizesAsc[i].Count
		capacity := p.sizesAsc[i].Capacity

		opts := p.opts
		if perBucketOpts := p.sizesAsc[i].Options; perBucketOpts != nil {
			opts = perBucketOpts
		}

		opts = opts.SetSize(size)
		scope := opts.InstrumentOptions().MetricsScope()
		iOpts := opts.InstrumentOptions().
			SetMetricsScope(scope.Tagged(map[string]string{
				"bucket-capacity": fmt.Sprintf("%d", capacity),
			}))
		opts = opts.SetInstrumentOptions(iOpts)

		buckets[i].capacity = capacity
		buckets[i].pool = NewFieldArrayPool(opts)
		buckets[i].pool.Init(func() []Field {
			return alloc(capacity)
		})
	}
	p.buckets = buckets
	p.alloc = alloc
}

// Get gets a value from the pool.
func (p *BucketizedFieldArrayPool) Get(capacity int) []Field {
	if capacity > p.maxBucketCapacity {
		p.maxAlloc.Inc(1)
		return p.alloc(capacity)
	}
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			return p.buckets[i].pool.Get()
		}
	}
	return p.alloc(capacity)
}

// Put puts a value to the pool.
func (p *BucketizedFieldArrayPool) Put(v []Field, capacity int) {
	if capacity > p.maxBucketCapacity {
		return
	}

	for i := len(p.buckets) - 1; i >= 0; i-- {
		if capacity >= p.buckets[i].capacity {
			p.buckets[i].pool.Put(v)
			return
		}
	}
}