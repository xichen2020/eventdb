package template

import (
	"fmt"
	"sort"

	"github.com/uber-go/tally"
)

// ValueBucket specifies a bucket.
type ValueBucket struct {
	// Capacity is the size of each element in the bucket.
	Capacity int

	// Count is the number of fixed elements in the bucket.
	Count int

	// Options is an optional override to specify options to use for a bucket,
	// specify nil to use the options specified to the bucketized pool
	// constructor for this bucket.
	Options *ValuePoolOptions
}

// valueBucketByCapacity is a sortable collection of pool buckets.
type valueBucketByCapacity []ValueBucket

func (x valueBucketByCapacity) Len() int {
	return len(x)
}

func (x valueBucketByCapacity) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x valueBucketByCapacity) Less(i, j int) bool {
	return x[i].Capacity < x[j].Capacity
}

type bucketPool struct {
	capacity int
	pool     *ValuePool
}

// BucketizedValuePool is a bucketized value pool.
type BucketizedValuePool struct {
	sizesAsc          []ValueBucket
	buckets           []bucketPool
	maxBucketCapacity int
	opts              *ValuePoolOptions
	alloc             func(capacity int) GenericValue
	maxAlloc          tally.Counter
}

// NewBucketizedValuePool creates a bucketized object pool.
func NewBucketizedValuePool(sizes []ValueBucket, opts *ValuePoolOptions) *BucketizedValuePool {
	if opts == nil {
		opts = NewValuePoolOptions()
	}

	sizesAsc := make([]ValueBucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(valueBucketByCapacity(sizesAsc))

	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}

	return &BucketizedValuePool{
		opts:              opts,
		sizesAsc:          sizesAsc,
		maxBucketCapacity: maxBucketCapacity,
		maxAlloc:          opts.InstrumentOptions().MetricsScope().Counter("alloc-max"),
	}
}

// Init initializes the bucketized pool.
func (p *BucketizedValuePool) Init(alloc func(capacity int) GenericValue) {
	buckets := make([]bucketPool, len(p.sizesAsc))
	for i := range p.sizesAsc {
		size := p.sizesAsc[i].Count
		capacity := p.sizesAsc[i].Capacity

		opts := p.opts
		if perBucketOpts := p.sizesAsc[i].Options; perBucketOpts != nil {
			opts = perBucketOpts
		}

		opts = opts.SetSize(size)
		iOpts := opts.InstrumentOptions()
		opts.SetInstrumentOptions(iOpts.SetMetricsScope(iOpts.MetricsScope().Tagged(map[string]string{
			"bucket-capacity": fmt.Sprintf("%d", capacity),
		})))

		buckets[i].capacity = capacity
		buckets[i].pool = NewValuePool(opts)
		buckets[i].pool.Init(func() GenericValue {
			return alloc(capacity)
		})
	}
	p.buckets = buckets
	p.alloc = alloc
}

// Get gets a value from the pool.
func (p *BucketizedValuePool) Get(capacity int) GenericValue {
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
func (p *BucketizedValuePool) Put(v GenericValue, capacity int) {
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
