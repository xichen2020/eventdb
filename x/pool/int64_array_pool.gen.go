// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package pool

import (
	"errors"

	"math"

	"sync/atomic"

	"github.com/uber-go/tally"
)

// Int64ArrayPoolOptions provide a set of options for the value pool.
type Int64ArrayPoolOptions struct {
	scope               tally.Scope
	size                int
	refillLowWatermark  float64
	refillHighWatermark float64
}

// NewInt64ArrayPoolOptions create a new set of value pool options.
func NewInt64ArrayPoolOptions() *Int64ArrayPoolOptions {
	return &Int64ArrayPoolOptions{
		scope: tally.NoopScope,
		size:  4096,
	}
}

// SetMetricsScope sets the metrics scope.
func (o *Int64ArrayPoolOptions) SetMetricsScope(v tally.Scope) *Int64ArrayPoolOptions {
	opts := *o
	opts.scope = v
	return &opts
}

// MetricsScope returns the metrics scope.
func (o *Int64ArrayPoolOptions) MetricsScope() tally.Scope { return o.scope }

// SetSize sets the pool size.
func (o *Int64ArrayPoolOptions) SetSize(v int) *Int64ArrayPoolOptions {
	opts := *o
	opts.size = v
	return &opts
}

// Size returns pool size.
func (o *Int64ArrayPoolOptions) Size() int { return o.size }

// SetRefillLowWatermark sets the low watermark for refilling the pool.
func (o *Int64ArrayPoolOptions) SetRefillLowWatermark(v float64) *Int64ArrayPoolOptions {
	opts := *o
	opts.refillLowWatermark = v
	return &opts
}

// RefillLowWatermark returns the low watermark for refilling the pool.
func (o *Int64ArrayPoolOptions) RefillLowWatermark() float64 { return o.refillLowWatermark }

// SetRefillHighWatermark sets the high watermark for refilling the pool.
func (o *Int64ArrayPoolOptions) SetRefillHighWatermark(v float64) *Int64ArrayPoolOptions {
	opts := *o
	opts.refillHighWatermark = v
	return &opts
}

// RefillHighWatermark returns the high watermark for stop refilling the pool.
func (o *Int64ArrayPoolOptions) RefillHighWatermark() float64 { return o.refillHighWatermark }

type int64ArrayPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

func newint64ArrayPoolMetrics(m tally.Scope) int64ArrayPoolMetrics {
	return int64ArrayPoolMetrics{
		free:       m.Gauge("free"),
		total:      m.Gauge("total"),
		getOnEmpty: m.Counter("get-on-empty"),
		putOnFull:  m.Counter("put-on-full"),
	}
}

// Int64ArrayPool is a value pool.
type Int64ArrayPool struct {
	values              chan []int64
	alloc               func() []int64
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
	dice                int32
	metrics             int64ArrayPoolMetrics
}

// NewInt64ArrayPool creates a new pool.
func NewInt64ArrayPool(opts *Int64ArrayPoolOptions) *Int64ArrayPool {
	if opts == nil {
		opts = NewInt64ArrayPoolOptions()
	}

	p := &Int64ArrayPool{
		values: make(chan []int64, opts.Size()),
		size:   opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: newint64ArrayPoolMetrics(opts.MetricsScope()),
	}

	p.setGauges()

	return p
}

// Init initializes the pool.
func (p *Int64ArrayPool) Init(alloc func() []int64) {
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
func (p *Int64ArrayPool) Get() []int64 {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("get before pool is initialized"))
	}

	var v []int64
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
func (p *Int64ArrayPool) Put(v []int64) {
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

func (p *Int64ArrayPool) trySetGauges() {
	if atomic.AddInt32(&p.dice, 1)%100 == 0 {
		p.setGauges()
	}
}

func (p *Int64ArrayPool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *Int64ArrayPool) tryFill() {
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