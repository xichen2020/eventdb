// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package pool

import (
	"errors"

	"math"

	"sync/atomic"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
)

// Float64ArrayPoolOptions provide a set of options for the value pool.
type Float64ArrayPoolOptions struct {
	instrumentOpts      instrument.Options
	size                int
	refillLowWatermark  float64
	refillHighWatermark float64
}

// NewFloat64ArrayPoolOptions create a new set of value pool options.
func NewFloat64ArrayPoolOptions() *Float64ArrayPoolOptions {
	return &Float64ArrayPoolOptions{
		instrumentOpts: instrument.NewOptions(),
		size:           4096,
	}
}

// SetInstrumentOptions sets the instrument options.
func (o *Float64ArrayPoolOptions) SetInstrumentOptions(v instrument.Options) *Float64ArrayPoolOptions {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *Float64ArrayPoolOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetSize sets the pool size.
func (o *Float64ArrayPoolOptions) SetSize(v int) *Float64ArrayPoolOptions {
	opts := *o
	opts.size = v
	return &opts
}

// Size returns pool size.
func (o *Float64ArrayPoolOptions) Size() int { return o.size }

// SetRefillLowWatermark sets the low watermark for refilling the pool.
func (o *Float64ArrayPoolOptions) SetRefillLowWatermark(v float64) *Float64ArrayPoolOptions {
	opts := *o
	opts.refillLowWatermark = v
	return &opts
}

// RefillLowWatermark returns the low watermark for refilling the pool.
func (o *Float64ArrayPoolOptions) RefillLowWatermark() float64 { return o.refillLowWatermark }

// SetRefillHighWatermark sets the high watermark for refilling the pool.
func (o *Float64ArrayPoolOptions) SetRefillHighWatermark(v float64) *Float64ArrayPoolOptions {
	opts := *o
	opts.refillHighWatermark = v
	return &opts
}

// RefillHighWatermark returns the high watermark for stop refilling the pool.
func (o *Float64ArrayPoolOptions) RefillHighWatermark() float64 { return o.refillHighWatermark }

type float64ArrayPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

func newfloat64ArrayPoolMetrics(m tally.Scope) float64ArrayPoolMetrics {
	return float64ArrayPoolMetrics{
		free:       m.Gauge("free"),
		total:      m.Gauge("total"),
		getOnEmpty: m.Counter("get-on-empty"),
		putOnFull:  m.Counter("put-on-full"),
	}
}

// Float64ArrayPool is a value pool.
type Float64ArrayPool struct {
	values              chan []float64
	alloc               func() []float64
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
	dice                int32
	metrics             float64ArrayPoolMetrics
}

// NewFloat64ArrayPool creates a new pool.
func NewFloat64ArrayPool(opts *Float64ArrayPoolOptions) *Float64ArrayPool {
	if opts == nil {
		opts = NewFloat64ArrayPoolOptions()
	}

	p := &Float64ArrayPool{
		values: make(chan []float64, opts.Size()),
		size:   opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: newfloat64ArrayPoolMetrics(opts.InstrumentOptions().MetricsScope()),
	}

	p.setGauges()

	return p
}

// Init initializes the pool.
func (p *Float64ArrayPool) Init(alloc func() []float64) {
	if !atomic.CompareAndSwapInt32(&p.initialized, 0, 1) {
		panic(errors.New("pool is already initialized"))
	}

	p.alloc = alloc

	for i := 0; i < cap(p.values); i++ {
		p.values <- p.alloc()
	}

	p.setGauges()
}

// Get gets a value from the pool.
func (p *Float64ArrayPool) Get() []float64 {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("get before pool is initialized"))
	}

	var v []float64
	select {
	case v = <-p.values:
	default:
		v = p.alloc()
		p.metrics.getOnEmpty.Inc(1)
	}

	p.trySetGauges()

	if p.refillLowWatermark > 0 && len(p.values) <= p.refillLowWatermark {
		p.tryFill()
	}

	return v
}

// Put returns a value to pool.
func (p *Float64ArrayPool) Put(v []float64) {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("put before pool is initialized"))
	}

	select {
	case p.values <- v:
	default:
		p.metrics.putOnFull.Inc(1)
	}

	p.trySetGauges()
}

func (p *Float64ArrayPool) trySetGauges() {
	if atomic.AddInt32(&p.dice, 1)%100 == 0 {
		p.setGauges()
	}
}

func (p *Float64ArrayPool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *Float64ArrayPool) tryFill() {
	if !atomic.CompareAndSwapInt32(&p.filling, 0, 1) {
		return
	}

	go func() {
		defer atomic.StoreInt32(&p.filling, 0)

		for len(p.values) < p.refillHighWatermark {
			select {
			case p.values <- p.alloc():
			default:
				return
			}
		}
	}()
}
