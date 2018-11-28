package storage

import (
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/xichen2020/eventdb/persist"
)

const (
	defaultNestedFieldSeparator = '.'
)

// Options provide a set of options for the database.
type Options struct {
	clockOpts            clock.Options
	instrumentOpts       instrument.Options
	nestedFieldSeparator byte
	persistManager       persist.Manager
}

// NewOptions create a new set of options.
func NewOptions() *Options {
	return &Options{
		clockOpts:            clock.NewOptions(),
		instrumentOpts:       instrument.NewOptions(),
		nestedFieldSeparator: defaultNestedFieldSeparator,
	}
}

// SetClockOptions sets the clock options.
func (o *Options) SetClockOptions(v clock.Options) *Options {
	opts := *o
	opts.clockOpts = v
	return &opts
}

// ClockOptions returns the clock options.
func (o *Options) ClockOptions() clock.Options {
	return o.clockOpts
}

// SetInstrumentOptions sets the instrument options.
func (o *Options) SetInstrumentOptions(v instrument.Options) *Options {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *Options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetPersistManager sets the persistence manager.
func (o *Options) SetPersistManager(v persist.Manager) *Options {
	opts := *o
	opts.persistManager = v
	return &opts
}

// PersistManager returns the persistence manager.
func (o *Options) PersistManager() persist.Manager {
	return o.persistManager
}

// SetNestedFieldSeparator sets the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) SetNestedFieldSeparator(v byte) *Options {
	opts := *o
	opts.nestedFieldSeparator = v
	return &opts
}

// NestedFieldSeparator returns the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) NestedFieldSeparator() byte {
	return o.nestedFieldSeparator
}
