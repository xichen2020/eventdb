package grpc

import (
	"time"

	"github.com/m3db/m3x/instrument"
)

const (
	defaultReadBufferSize  = 1024 * 1024      // 1MB
	defaultMaxRecvMsgSize  = 64 * 1024 * 1024 // 64MB
	defaultKeepAlivePeriod = 0
)

// Options provide a set of server options.
type Options struct {
	instrumentOpts  instrument.Options
	readBufferSize  int
	maxRecvMsgSize  int
	keepAlivePeriod time.Duration
}

// NewOptions creates a new set of options.
func NewOptions() *Options {
	return &Options{
		instrumentOpts:  instrument.NewOptions(),
		readBufferSize:  defaultReadBufferSize,
		maxRecvMsgSize:  defaultMaxRecvMsgSize,
		keepAlivePeriod: defaultKeepAlivePeriod,
	}
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

// SetReadBufferSize sets the read buffer size. This determines how much data can be read
// at most for one read syscall.
func (o *Options) SetReadBufferSize(v int) *Options {
	opts := *o
	opts.readBufferSize = v
	return &opts
}

// ReadBufferSize returns the read buffer size. This determines how much data can be read
// at most for one read syscall.
func (o *Options) ReadBufferSize() int {
	return o.readBufferSize
}

// SetMaxRecvMsgSize sets the max message size in bytes the server can receive.
func (o *Options) SetMaxRecvMsgSize(v int) *Options {
	opts := *o
	opts.maxRecvMsgSize = v
	return &opts
}

// MaxRecvMsgSize returns the max message size in bytes the server can receive.
func (o *Options) MaxRecvMsgSize() int {
	return o.maxRecvMsgSize
}

// SetKeepAlivePeriod sets the keep alive period.
func (o *Options) SetKeepAlivePeriod(v time.Duration) *Options {
	opts := *o
	opts.keepAlivePeriod = v
	return &opts
}

// KeepAlivePeriod returns the keep alive period.
func (o *Options) KeepAlivePeriod() time.Duration {
	return o.keepAlivePeriod
}
