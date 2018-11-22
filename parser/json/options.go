package json

import "math"

const (
	// By default the full JSON message is parsed.
	defaultMaxDepth = math.MaxInt64
)

// Options provide a set of parsing options.
// TODO(xichen): limit the maximum capacity of each caching array via options.
// TODO(xichen): limit the maximum capacity of cached value arrays and KV arrays via options.
type Options struct {
	maxDepth int
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
