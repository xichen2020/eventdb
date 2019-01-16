// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package pool

import (
	"github.com/m3db/m3x/instrument"
)

// Int64ArrayBucketConfiguration contains configuration for a pool bucket.
type Int64ArrayBucketConfiguration struct {
	// The count of the items in the bucket.
	Count int `yaml:"count"`

	// The capacity of each item in the bucket.
	Capacity int `yaml:"capacity"`
}

// NewBucket creates a new bucket.
func (c *Int64ArrayBucketConfiguration) NewBucket() Int64ArrayBucket {
	return Int64ArrayBucket{
		Capacity: c.Capacity,
		Count:    c.Count,
	}
}

// BucketizedInt64ArrayPoolConfiguration contains configuration for bucketized pools.
type BucketizedInt64ArrayPoolConfiguration struct {
	// The pool bucket configuration.
	Buckets []Int64ArrayBucketConfiguration `yaml:"buckets"`

	// The watermark configuration.
	Watermark Int64ArrayPoolWatermarkConfiguration `yaml:"watermark"`
}

// NewPoolOptions creates a new set of pool options.
func (c *BucketizedInt64ArrayPoolConfiguration) NewPoolOptions(
	instrumentOptions instrument.Options,
) *Int64ArrayPoolOptions {
	return NewInt64ArrayPoolOptions().
		SetInstrumentOptions(instrumentOptions).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
}

// NewBuckets create a new list of buckets.
func (c *BucketizedInt64ArrayPoolConfiguration) NewBuckets() []Int64ArrayBucket {
	buckets := make([]Int64ArrayBucket, 0, len(c.Buckets))
	for _, bconfig := range c.Buckets {
		bucket := bconfig.NewBucket()
		buckets = append(buckets, bucket)
	}
	return buckets
}
