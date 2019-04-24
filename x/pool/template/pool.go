package template

import (
	"errors"
	"math"
	"sync/atomic"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/mauricelam/genny/generic"
	"github.com/uber-go/tally"
)

// GenericValue is a value type.
type GenericValue generic.Type

// ValuePoolOptions provide a set of options for the value pool.
type ValuePoolOptions struct {
	instrumentOpts      instrument.Options
	size                int
	refillLowWatermark  float64
	refillHighWatermark float64
}

// NewValuePoolOptions create a new set of value pool options.
func NewValuePoolOptions() *ValuePoolOptions {
	return &ValuePoolOptions{
		instrumentOpts: instrument.NewOptions(),
		size:           4096,
	}
}

// SetInstrumentOptions sets the instrument options.
func (o *ValuePoolOptions) SetInstrumentOptions(v instrument.Options) *ValuePoolOptions {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *ValuePoolOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetSize sets the pool size.
func (o *ValuePoolOptions) SetSize(v int) *ValuePoolOptions {
	opts := *o
	opts.size = v
	return &opts
}

// Size returns pool size.
func (o *ValuePoolOptions) Size() int { return o.size }

// SetRefillLowWatermark sets the low watermark for refilling the pool.
func (o *ValuePoolOptions) SetRefillLowWatermark(v float64) *ValuePoolOptions {
	opts := *o
	opts.refillLowWatermark = v
	return &opts
}

// RefillLowWatermark returns the low watermark for refilling the pool.
func (o *ValuePoolOptions) RefillLowWatermark() float64 { return o.refillLowWatermark }

// SetRefillHighWatermark sets the high watermark for refilling the pool.
func (o *ValuePoolOptions) SetRefillHighWatermark(v float64) *ValuePoolOptions {
	opts := *o
	opts.refillHighWatermark = v
	return &opts
}

// RefillHighWatermark returns the high watermark for stop refilling the pool.
func (o *ValuePoolOptions) RefillHighWatermark() float64 { return o.refillHighWatermark }

type valuePoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

func newValuePoolMetrics(m tally.Scope) valuePoolMetrics {
	return valuePoolMetrics{
		free:       m.Gauge("free"),
		total:      m.Gauge("total"),
		getOnEmpty: m.Counter("get-on-empty"),
		putOnFull:  m.Counter("put-on-full"),
	}
}

// ValuePool is a value pool.
type ValuePool struct {
	values              chan GenericValue
	alloc               func() GenericValue
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
	dice                int32
	metrics             valuePoolMetrics
}

// NewValuePool creates a new pool.
func NewValuePool(opts *ValuePoolOptions) *ValuePool {
	if opts == nil {
		opts = NewValuePoolOptions()
	}

	p := &ValuePool{
		values: make(chan GenericValue, opts.Size()),
		size:   opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: newValuePoolMetrics(opts.InstrumentOptions().MetricsScope()),
	}

	p.setGauges()

	return p
}

// Init initializes the pool.
func (p *ValuePool) Init(alloc func() GenericValue) {
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
func (p *ValuePool) Get() GenericValue {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("get before pool is initialized"))
	}

	var v GenericValue
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
func (p *ValuePool) Put(v GenericValue) {
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

func (p *ValuePool) trySetGauges() {
	if atomic.AddInt32(&p.dice, 1)%100 == 0 {
		p.setGauges()
	}
}

func (p *ValuePool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *ValuePool) tryFill() {
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
