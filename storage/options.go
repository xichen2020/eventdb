package storage

import (
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/xichen2020/eventdb/persist"
)

const (
	defaultFieldPathSeparator           = '.'
	defaultMaxNumCachedSegmentsPerShard = 1
)

// Options provide a set of options for the database.
type Options struct {
	clockOpts                    clock.Options
	instrumentOpts               instrument.Options
	fieldPathSeparator           byte
	persistManager               persist.Manager
	maxNumCachedSegmentsPerShard int
}

// NewOptions create a new set of options.
func NewOptions() *Options {
	return &Options{
		clockOpts:                    clock.NewOptions(),
		instrumentOpts:               instrument.NewOptions(),
		fieldPathSeparator:           defaultFieldPathSeparator,
		maxNumCachedSegmentsPerShard: defaultMaxNumCachedSegmentsPerShard,
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

// SetFieldPathSeparator sets the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) SetFieldPathSeparator(v byte) *Options {
	opts := *o
	opts.fieldPathSeparator = v
	return &opts
}

// FieldPathSeparator returns the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) FieldPathSeparator() byte {
	return o.fieldPathSeparator
}

// SetMaxNumCachedSegmentsPerShard sets the maximum number of segments cached in
// memory per shard in a namespace.
func (o *Options) SetMaxNumCachedSegmentsPerShard(v int) *Options {
	opts := *o
	opts.maxNumCachedSegmentsPerShard = v
	return &opts
}

// MaxNumCachedSegmentsPerShard returns the maximum number of segments cached in
// memory per shard in a namespace.
func (o *Options) MaxNumCachedSegmentsPerShard() int {
	return o.maxNumCachedSegmentsPerShard
}
