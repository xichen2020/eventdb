// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package json

import (
	"errors"

	"math"

	"sync/atomic"

	"github.com/uber-go/tally"
)

// ParserPoolOptions provide a set of options for the value pool.
type ParserPoolOptions struct {
	scope               tally.Scope
	size                int
	refillLowWatermark  float64
	refillHighWatermark float64
}

// NewParserPoolOptions create a new set of value pool options.
func NewParserPoolOptions() *ParserPoolOptions {
	return &ParserPoolOptions{
		scope: tally.NoopScope,
		size:  4096,
	}
}

// SetMetricsScope sets the metrics scope.
func (o *ParserPoolOptions) SetMetricsScope(v tally.Scope) *ParserPoolOptions {
	opts := *o
	opts.scope = v
	return &opts
}

// MetricsScope returns the metrics scope.
func (o *ParserPoolOptions) MetricsScope() tally.Scope { return o.scope }

// SetSize sets the pool size.
func (o *ParserPoolOptions) SetSize(v int) *ParserPoolOptions {
	opts := *o
	opts.size = v
	return &opts
}

// Size returns pool size.
func (o *ParserPoolOptions) Size() int { return o.size }

// SetRefillLowWatermark sets the low watermark for refilling the pool.
func (o *ParserPoolOptions) SetRefillLowWatermark(v float64) *ParserPoolOptions {
	opts := *o
	opts.refillLowWatermark = v
	return &opts
}

// RefillLowWatermark returns the low watermark for refilling the pool.
func (o *ParserPoolOptions) RefillLowWatermark() float64 { return o.refillLowWatermark }

// SetRefillHighWatermark sets the high watermark for refilling the pool.
func (o *ParserPoolOptions) SetRefillHighWatermark(v float64) *ParserPoolOptions {
	opts := *o
	opts.refillHighWatermark = v
	return &opts
}

// RefillHighWatermark returns the high watermark for stop refilling the pool.
func (o *ParserPoolOptions) RefillHighWatermark() float64 { return o.refillHighWatermark }

type parserPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

func newparserPoolMetrics(m tally.Scope) parserPoolMetrics {
	return parserPoolMetrics{
		free:       m.Gauge("free"),
		total:      m.Gauge("total"),
		getOnEmpty: m.Counter("get-on-empty"),
		putOnFull:  m.Counter("put-on-full"),
	}
}

// ParserPool is a value pool.
type ParserPool struct {
	values              chan Parser
	alloc               func() Parser
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
	dice                int32
	metrics             parserPoolMetrics
}

// NewParserPool creates a new pool.
func NewParserPool(opts *ParserPoolOptions) *ParserPool {
	if opts == nil {
		opts = NewParserPoolOptions()
	}

	p := &ParserPool{
		values: make(chan Parser, opts.Size()),
		size:   opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: newparserPoolMetrics(opts.MetricsScope()),
	}

	p.setGauges()

	return p
}

// Init initializes the pool.
func (p *ParserPool) Init(alloc func() Parser) {
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
func (p *ParserPool) Get() Parser {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("get before pool is initialized"))
	}

	var v Parser
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
func (p *ParserPool) Put(v Parser) {
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

func (p *ParserPool) trySetGauges() {
	if atomic.AddInt32(&p.dice, 1)%100 == 0 {
		p.setGauges()
	}
}

func (p *ParserPool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *ParserPool) tryFill() {
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
