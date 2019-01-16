// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package pool

import (
	"errors"

	"math"

	"sync/atomic"

	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

// BoolArrayPoolOptions provide a set of options for the value pool.
type BoolArrayPoolOptions struct {
	instrumentOpts      instrument.Options
	size                int
	refillLowWatermark  float64
	refillHighWatermark float64
}

// NewBoolArrayPoolOptions create a new set of value pool options.
func NewBoolArrayPoolOptions() *BoolArrayPoolOptions {
	return &BoolArrayPoolOptions{
		instrumentOpts: instrument.NewOptions(),
		size:           4096,
	}
}

// SetInstrumentOptions sets the instrument options.
func (o *BoolArrayPoolOptions) SetInstrumentOptions(v instrument.Options) *BoolArrayPoolOptions {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *BoolArrayPoolOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetSize sets the pool size.
func (o *BoolArrayPoolOptions) SetSize(v int) *BoolArrayPoolOptions {
	opts := *o
	opts.size = v
	return &opts
}

// Size returns pool size.
func (o *BoolArrayPoolOptions) Size() int { return o.size }

// SetRefillLowWatermark sets the low watermark for refilling the pool.
func (o *BoolArrayPoolOptions) SetRefillLowWatermark(v float64) *BoolArrayPoolOptions {
	opts := *o
	opts.refillLowWatermark = v
	return &opts
}

// RefillLowWatermark returns the low watermark for refilling the pool.
func (o *BoolArrayPoolOptions) RefillLowWatermark() float64 { return o.refillLowWatermark }

// SetRefillHighWatermark sets the high watermark for refilling the pool.
func (o *BoolArrayPoolOptions) SetRefillHighWatermark(v float64) *BoolArrayPoolOptions {
	opts := *o
	opts.refillHighWatermark = v
	return &opts
}

// RefillHighWatermark returns the high watermark for stop refilling the pool.
func (o *BoolArrayPoolOptions) RefillHighWatermark() float64 { return o.refillHighWatermark }

type boolArrayPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

func newboolArrayPoolMetrics(m tally.Scope) boolArrayPoolMetrics {
	return boolArrayPoolMetrics{
		free:       m.Gauge("free"),
		total:      m.Gauge("total"),
		getOnEmpty: m.Counter("get-on-empty"),
		putOnFull:  m.Counter("put-on-full"),
	}
}

// BoolArrayPool is a value pool.
type BoolArrayPool struct {
	values              chan []bool
	alloc               func() []bool
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
	dice                int32
	metrics             boolArrayPoolMetrics
}

// NewBoolArrayPool creates a new pool.
func NewBoolArrayPool(opts *BoolArrayPoolOptions) *BoolArrayPool {
	if opts == nil {
		opts = NewBoolArrayPoolOptions()
	}

	p := &BoolArrayPool{
		values: make(chan []bool, opts.Size()),
		size:   opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: newboolArrayPoolMetrics(opts.InstrumentOptions().MetricsScope()),
	}

	p.setGauges()

	return p
}

// Init initializes the pool.
func (p *BoolArrayPool) Init(alloc func() []bool) {
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
func (p *BoolArrayPool) Get() []bool {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("get before pool is initialized"))
	}

	var v []bool
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
func (p *BoolArrayPool) Put(v []bool) {
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

func (p *BoolArrayPool) trySetGauges() {
	if atomic.AddInt32(&p.dice, 1)%100 == 0 {
		p.setGauges()
	}
}

func (p *BoolArrayPool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *BoolArrayPool) tryFill() {
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
