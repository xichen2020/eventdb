package json

import "math"

const (
	// By default the full JSON message is parsed.
	defaultMaxDepth = math.MaxInt64
)

// ObjectKeyFilterFn filters out values associated with keys matching the filter.
type ObjectKeyFilterFn func(key string) bool

// Options provide a set of parsing options.
// TODO(xichen): limit the maximum capacity of each caching array via options.
// TODO(xichen): limit the maximum capacity of cached value arrays and KV arrays via options.
type Options struct {
	maxDepth          int
	objectKeyFilterFn ObjectKeyFilterFn
}

// NewOptions creates a new set of parsing options.
func NewOptions() *Options {
	return &Options{
		maxDepth: defaultMaxDepth,
	}
}

// SetMaxDepth sets the maximum depth eligible for parsing.
func (o *Options) SetMaxDepth(v int) *Options {
	opts := *o
	opts.maxDepth = v
	return &opts
}

// MaxDepth returns the maximum depth eligible for parsing.
func (o *Options) MaxDepth() int { return o.maxDepth }

// SetObjectKeyFilterFn sets the object key matching function.
func (o *Options) SetObjectKeyFilterFn(v ObjectKeyFilterFn) *Options {
	opts := *o
	opts.objectKeyFilterFn = v
	return &opts
}

// ObjectKeyFilterFn returns the object key matching function.
func (o *Options) ObjectKeyFilterFn() ObjectKeyFilterFn { return o.objectKeyFilterFn }
