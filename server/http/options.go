package http

import (
	"time"
)

const (
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
)

// Options provide a set of HTTP server options.
type Options struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewOptions creates a new set of server options.
func NewOptions() *Options {
	o := &Options{
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}
	return o
}

// SetReadTimeout sets the timeout for a read request.
func (o *Options) SetReadTimeout(value time.Duration) *Options {
	opts := *o
	opts.readTimeout = value
	return &opts
}

// ReadTimeout returns the timeout for a read request.
func (o *Options) ReadTimeout() time.Duration {
	return o.readTimeout
}

// SetWriteTimeout sets the timeout for a write request.
func (o *Options) SetWriteTimeout(value time.Duration) *Options {
	opts := *o
	opts.writeTimeout = value
	return &opts
}

// WriteTimeout returns the timeout for a write request.
func (o *Options) WriteTimeout() time.Duration {
	return o.writeTimeout
}
