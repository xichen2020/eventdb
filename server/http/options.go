package http

import (
	"github.com/m3db/m3x/instrument"
)

// Options provide a set of HTTP server options.
type Options struct {
	instrumentOpts instrument.Options
}

// NewOptions creates a new set of server options.
func NewOptions() *Options {
	o := &Options{
		instrumentOpts: instrument.NewOptions(),
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
