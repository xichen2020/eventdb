// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package pool

import (
	"github.com/uber-go/tally"
)

// IntArrayBucketConfiguration contains configuration for a pool bucket.
type IntArrayBucketConfiguration struct {
	// The count of the items in the bucket.
	Count int `yaml:"count"`

	// The capacity of each item in the bucket.
	Capacity int `yaml:"capacity"`
}

// NewBucket creates a new bucket.
func (c *IntArrayBucketConfiguration) NewBucket() IntArrayBucket {
	return IntArrayBucket{
		Capacity: c.Capacity,
		Count:    c.Count,
	}
}

// BucketizedIntArrayPoolConfiguration contains configuration for bucketized pools.
type BucketizedIntArrayPoolConfiguration struct {
	// The pool bucket configuration.
	Buckets []IntArrayBucketConfiguration `yaml:"buckets"`

	// The watermark configuration.
	Watermark IntArrayPoolWatermarkConfiguration `yaml:"watermark"`
}

// NewPoolOptions creates a new set of pool options.
func (c *BucketizedIntArrayPoolConfiguration) NewPoolOptions(
	scope tally.Scope,
) *IntArrayPoolOptions {
	return NewIntArrayPoolOptions().
		SetMetricsScope(scope).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
}

// NewBuckets create a new list of buckets.
func (c *BucketizedIntArrayPoolConfiguration) NewBuckets() []IntArrayBucket {
	buckets := make([]IntArrayBucket, 0, len(c.Buckets))
	for _, bconfig := range c.Buckets {
		bucket := bconfig.NewBucket()
		buckets = append(buckets, bucket)
	}
	return buckets
}
