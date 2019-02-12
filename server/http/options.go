package http

import (
	"time"

	"github.com/m3db/m3x/instrument"
)

const (
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
)

// Options provide a set of HTTP server options.
type Options struct {
	instrumentOpts instrument.Options
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

// NewOptions creates a new set of server options.
func NewOptions() *Options {
	o := &Options{
		instrumentOpts: instrument.NewOptions(),
		readTimeout:    defaultReadTimeout,
		writeTimeout:   defaultWriteTimeout,
	}
	return o
}

// SetInstrumentOptions sets the instrument options.
func (o *Options) SetInstrumentOptions(value instrument.Options) *Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *Options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
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
