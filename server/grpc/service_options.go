package grpc

import (
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultReadTimeout  = time.Minute
	defaultWriteTimeout = time.Minute
)

// ServiceOptions provide a set of service options.
type ServiceOptions struct {
	clockOpts         clock.Options
	instrumentOpts    instrument.Options
	readTimeout       time.Duration
	writeTimeout      time.Duration
	documentArrayPool *document.BucketizedDocumentArrayPool
	fieldArrayPool    *field.BucketizedFieldArrayPool
}

// NewServiceOptions create a new set of service options.
func NewServiceOptions() *ServiceOptions {
	o := &ServiceOptions{
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		readTimeout:    defaultReadTimeout,
		writeTimeout:   defaultWriteTimeout,
	}
	o.initPools()
	return o
}

// SetClockOptions sets the clock options.
func (o *ServiceOptions) SetClockOptions(v clock.Options) *ServiceOptions {
	opts := *o
	opts.clockOpts = v
	return &opts
}

// ClockOptions returns the clock options.
func (o *ServiceOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

// SetInstrumentOptions sets the instrument options.
func (o *ServiceOptions) SetInstrumentOptions(v instrument.Options) *ServiceOptions {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *ServiceOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetReadTimeout sets the read timeout.
func (o *ServiceOptions) SetReadTimeout(v time.Duration) *ServiceOptions {
	opts := *o
	opts.readTimeout = v
	return &opts
}

// ReadTimeout returns the read timeout.
func (o *ServiceOptions) ReadTimeout() time.Duration {
	return o.readTimeout
}

// SetWriteTimeout sets the write timeout.
func (o *ServiceOptions) SetWriteTimeout(v time.Duration) *ServiceOptions {
	opts := *o
	opts.writeTimeout = v
	return &opts
}

// WriteTimeout returns the write timeout.
func (o *ServiceOptions) WriteTimeout() time.Duration {
	return o.writeTimeout
}

// SetDocumentArrayPool sets the document array pool.
func (o *ServiceOptions) SetDocumentArrayPool(v *document.BucketizedDocumentArrayPool) *ServiceOptions {
	opts := *o
	opts.documentArrayPool = v
	return &opts
}

// DocumentArrayPool returns the document array pool.
func (o *ServiceOptions) DocumentArrayPool() *document.BucketizedDocumentArrayPool {
	return o.documentArrayPool
}

// SetFieldArrayPool sets the field array pool.
func (o *ServiceOptions) SetFieldArrayPool(v *field.BucketizedFieldArrayPool) *ServiceOptions {
	opts := *o
	opts.fieldArrayPool = v
	return &opts
}

// FieldArrayPool returns the field array pool.
func (o *ServiceOptions) FieldArrayPool() *field.BucketizedFieldArrayPool {
	return o.fieldArrayPool
}

func (o *ServiceOptions) initPools() {
	documentArrayPool := document.NewBucketizedDocumentArrayPool(nil, nil)
	documentArrayPool.Init(func(capacity int) []document.Document {
		return make([]document.Document, 0, capacity)
	})
	o.documentArrayPool = documentArrayPool

	fieldArrayPool := field.NewBucketizedFieldArrayPool(nil, nil)
	fieldArrayPool.Init(func(capacity int) []field.Field {
		return make([]field.Field, 0, capacity)
	})
	o.fieldArrayPool = fieldArrayPool
}
