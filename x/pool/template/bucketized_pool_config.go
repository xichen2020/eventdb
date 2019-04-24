package template

import "github.com/m3db/m3/src/x/instrument"

// ValuePoolBucketConfiguration contains configuration for a pool bucket.
type ValuePoolBucketConfiguration struct {
	// The count of the items in the bucket.
	Count int `yaml:"count"`

	// The capacity of each item in the bucket.
	Capacity int `yaml:"capacity"`
}

// NewBucket creates a new bucket.
func (c *ValuePoolBucketConfiguration) NewBucket() ValueBucket {
	return ValueBucket{
		Capacity: c.Capacity,
		Count:    c.Count,
	}
}

// BucketizedValuePoolConfiguration contains configuration for bucketized pools.
type BucketizedValuePoolConfiguration struct {
	// The pool bucket configuration.
	Buckets []ValuePoolBucketConfiguration `yaml:"buckets"`

	// The watermark configuration.
	Watermark ValuePoolWatermarkConfiguration `yaml:"watermark"`
}

// NewPoolOptions creates a new set of pool options.
func (c *BucketizedValuePoolConfiguration) NewPoolOptions(
	instrumentOptions instrument.Options,
) *ValuePoolOptions {
	return NewValuePoolOptions().
		SetInstrumentOptions(instrumentOptions).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
}

// NewBuckets create a new list of buckets.
func (c *BucketizedValuePoolConfiguration) NewBuckets() []ValueBucket {
	buckets := make([]ValueBucket, 0, len(c.Buckets))
	for _, bconfig := range c.Buckets {
		bucket := bconfig.NewBucket()
		buckets = append(buckets, bucket)
	}
	return buckets
}
