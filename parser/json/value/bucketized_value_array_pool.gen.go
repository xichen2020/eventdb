// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package value

import (
	"fmt"

	"sort"

	"github.com/uber-go/tally"
)

// ArrayBucket specifies a bucket.
type ArrayBucket struct {
	// Capacity is the size of each element in the bucket.
	Capacity int

	// Count is the number of fixed elements in the bucket.
	Count int

	// Options is an optional override to specify options to use for a bucket,
	// specify nil to use the options specified to the bucketized pool
	// constructor for this bucket.
	Options *ArrayPoolOptions
}

// arrayBucketByCapacity is a sortable collection of pool buckets.
type arrayBucketByCapacity []ArrayBucket

func (x arrayBucketByCapacity) Len() int {
	return len(x)
}

func (x arrayBucketByCapacity) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x arrayBucketByCapacity) Less(i, j int) bool {
	return x[i].Capacity < x[j].Capacity
}

type arrayBucketPool struct {
	capacity int
	pool     *ArrayPool
}

// BucketizedArrayPool is a bucketized value pool.
type BucketizedArrayPool struct {
	sizesAsc          []ArrayBucket
	buckets           []arrayBucketPool
	maxBucketCapacity int
	opts              *ArrayPoolOptions
	alloc             func(capacity int) Array
	maxAlloc          tally.Counter
}

// NewBucketizedArrayPool creates a bucketized object pool.
func NewBucketizedArrayPool(sizes []ArrayBucket, opts *ArrayPoolOptions) *BucketizedArrayPool {
	if opts == nil {
		opts = NewArrayPoolOptions()
	}

	sizesAsc := make([]ArrayBucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(arrayBucketByCapacity(sizesAsc))

	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}

	return &BucketizedArrayPool{
		opts:              opts,
		sizesAsc:          sizesAsc,
		maxBucketCapacity: maxBucketCapacity,
		maxAlloc:          opts.InstrumentOptions().MetricsScope().Counter("alloc-max"),
	}
}

// Init initializes the bucketized pool.
func (p *BucketizedArrayPool) Init(alloc func(capacity int) Array) {
	buckets := make([]arrayBucketPool, len(p.sizesAsc))
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
		opts.SetInstrumentOptions(iOpts)

		buckets[i].capacity = capacity
		buckets[i].pool = NewArrayPool(opts)
		buckets[i].pool.Init(func() Array {
			return alloc(capacity)
		})
	}
	p.buckets = buckets
	p.alloc = alloc
}

// Get gets a value from the pool.
func (p *BucketizedArrayPool) Get(capacity int) Array {
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
func (p *BucketizedArrayPool) Put(v Array, capacity int) {
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
