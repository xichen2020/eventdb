package grpc

import (
	"time"

	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	defaultDialTimeout     = 2 * time.Second
	defaultUseInsecure     = true
	defaultUseCompression  = true
	defaultWriteBufferSize = 1024 * 1024 // 1MB
	defaultMaxRecvMsgSize  = 1024 * 1024 // 1MB
	defaultKeepAlivePeriod = 0
	defaultReadTimeout     = time.Minute
	defaultWriteTimeout    = time.Minute
)

// Options provide a set of client options.
type Options struct {
	clockOpts       clock.Options
	instrumentOpts  instrument.Options
	dialTimeout     time.Duration
	useInsecure     bool
	useCompression  bool
	writeBufferSize int
	maxRecvMsgSize  int
	keepAlivePeriod time.Duration
	readTimeout     time.Duration
	writeTimeout    time.Duration
}

// NewOptions creates a new set of options.
func NewOptions() *Options {
	return &Options{
		clockOpts:       clock.NewOptions(),
		instrumentOpts:  instrument.NewOptions(),
		dialTimeout:     defaultDialTimeout,
		useInsecure:     defaultUseInsecure,
		useCompression:  defaultUseCompression,
		writeBufferSize: defaultWriteBufferSize,
		maxRecvMsgSize:  defaultMaxRecvMsgSize,
		keepAlivePeriod: defaultKeepAlivePeriod,
		readTimeout:     defaultReadTimeout,
		writeTimeout:    defaultWriteTimeout,
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

// SetDialTimeout sets the dial timeout for establishing the initial connection.
// When this is non zero, the initial dialing is blocking until the connection is established.
func (o *Options) SetDialTimeout(v time.Duration) *Options {
	opts := *o
	opts.dialTimeout = v
	return &opts
}

// DialTimeout returns the dial timeout.
// When this is non zero, the initial dialing is blocking until the connection is established.
func (o *Options) DialTimeout() time.Duration {
	return o.dialTimeout
}

// SetUseInsecure sets whether to use insecure connection.
func (o *Options) SetUseInsecure(v bool) *Options {
	opts := *o
	opts.useInsecure = v
	return &opts
}

// UseInsecure returns whether to use insecure connection.
func (o *Options) UseInsecure() bool {
	return o.useInsecure
}

// SetUseCompression sets whether to use compression.
func (o *Options) SetUseCompression(v bool) *Options {
	opts := *o
	opts.useCompression = v
	return &opts
}

// UseCompression returns whether to use compression.
func (o *Options) UseCompression() bool {
	return o.useCompression
}

// SetWriteBufferSize sets the read buffer size. This determines how much data can be read
// at most for one read syscall.
func (o *Options) SetWriteBufferSize(v int) *Options {
	opts := *o
	opts.writeBufferSize = v
	return &opts
}

// WriteBufferSize returns the read buffer size. This determines how much data can be read
// at most for one read syscall.
func (o *Options) WriteBufferSize() int {
	return o.writeBufferSize
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

// SetReadTimeout sets the read timeout.
func (o *Options) SetReadTimeout(v time.Duration) *Options {
	opts := *o
	opts.readTimeout = v
	return &opts
}

// ReadTimeout returns the read timeout.
func (o *Options) ReadTimeout() time.Duration {
	return o.readTimeout
}

// SetWriteTimeout sets the write timeout.
func (o *Options) SetWriteTimeout(v time.Duration) *Options {
	opts := *o
	opts.writeTimeout = v
	return &opts
}

// WriteTimeout returns the write timeout.
func (o *Options) WriteTimeout() time.Duration {
	return o.writeTimeout
}
