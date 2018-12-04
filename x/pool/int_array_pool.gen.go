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

// IntArrayPoolOptions provide a set of options for the value pool.
type IntArrayPoolOptions struct {
	scope               tally.Scope
	size                int
	refillLowWatermark  float64
	refillHighWatermark float64
}

// NewIntArrayPoolOptions create a new set of value pool options.
func NewIntArrayPoolOptions() *IntArrayPoolOptions {
	return &IntArrayPoolOptions{
		scope: tally.NoopScope,
		size:  4096,
	}
}

// SetMetricsScope sets the metrics scope.
func (o *IntArrayPoolOptions) SetMetricsScope(v tally.Scope) *IntArrayPoolOptions {
	opts := *o
	opts.scope = v
	return &opts
}

// MetricsScope returns the metrics scope.
func (o *IntArrayPoolOptions) MetricsScope() tally.Scope { return o.scope }

// SetSize sets the pool size.
func (o *IntArrayPoolOptions) SetSize(v int) *IntArrayPoolOptions {
	opts := *o
	opts.size = v
	return &opts
}

// Size returns pool size.
func (o *IntArrayPoolOptions) Size() int { return o.size }

// SetRefillLowWatermark sets the low watermark for refilling the pool.
func (o *IntArrayPoolOptions) SetRefillLowWatermark(v float64) *IntArrayPoolOptions {
	opts := *o
	opts.refillLowWatermark = v
	return &opts
}

// RefillLowWatermark returns the low watermark for refilling the pool.
func (o *IntArrayPoolOptions) RefillLowWatermark() float64 { return o.refillLowWatermark }

// SetRefillHighWatermark sets the high watermark for refilling the pool.
func (o *IntArrayPoolOptions) SetRefillHighWatermark(v float64) *IntArrayPoolOptions {
	opts := *o
	opts.refillHighWatermark = v
	return &opts
}

// RefillHighWatermark returns the high watermark for stop refilling the pool.
func (o *IntArrayPoolOptions) RefillHighWatermark() float64 { return o.refillHighWatermark }

type intArrayPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

func newintArrayPoolMetrics(m tally.Scope) intArrayPoolMetrics {
	return intArrayPoolMetrics{
		free:       m.Gauge("free"),
		total:      m.Gauge("total"),
		getOnEmpty: m.Counter("get-on-empty"),
		putOnFull:  m.Counter("put-on-full"),
	}
}

// IntArrayPool is a value pool.
type IntArrayPool struct {
	values              chan []int
	alloc               func() []int
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
	dice                int32
	metrics             intArrayPoolMetrics
}

// NewIntArrayPool creates a new pool.
func NewIntArrayPool(opts *IntArrayPoolOptions) *IntArrayPool {
	if opts == nil {
		opts = NewIntArrayPoolOptions()
	}

	p := &IntArrayPool{
		values: make(chan []int, opts.Size()),
		size:   opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: newintArrayPoolMetrics(opts.MetricsScope()),
	}

	p.setGauges()

	return p
}

// Init initializes the pool.
func (p *IntArrayPool) Init(alloc func() []int) {
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
func (p *IntArrayPool) Get() []int {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("get before pool is initialized"))
	}

	var v []int
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
func (p *IntArrayPool) Put(v []int) {
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

func (p *IntArrayPool) trySetGauges() {
	if atomic.AddInt32(&p.dice, 1)%100 == 0 {
		p.setGauges()
	}
}

func (p *IntArrayPool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *IntArrayPool) tryFill() {
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