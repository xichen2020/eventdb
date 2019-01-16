// Copyright (c) 2019 Uber Technologies, Inc.
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

package value

import (
	"errors"

	"math"

	"sync/atomic"

	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

// KVArrayPoolOptions provide a set of options for the value pool.
type KVArrayPoolOptions struct {
	instrumentOpts      instrument.Options
	size                int
	refillLowWatermark  float64
	refillHighWatermark float64
}

// NewKVArrayPoolOptions create a new set of value pool options.
func NewKVArrayPoolOptions() *KVArrayPoolOptions {
	return &KVArrayPoolOptions{
		instrumentOpts: instrument.NewOptions(),
		size:           4096,
	}
}

// SetInstrumentOptions sets the instrument options.
func (o *KVArrayPoolOptions) SetInstrumentOptions(v instrument.Options) *KVArrayPoolOptions {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *KVArrayPoolOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetSize sets the pool size.
func (o *KVArrayPoolOptions) SetSize(v int) *KVArrayPoolOptions {
	opts := *o
	opts.size = v
	return &opts
}

// Size returns pool size.
func (o *KVArrayPoolOptions) Size() int { return o.size }

// SetRefillLowWatermark sets the low watermark for refilling the pool.
func (o *KVArrayPoolOptions) SetRefillLowWatermark(v float64) *KVArrayPoolOptions {
	opts := *o
	opts.refillLowWatermark = v
	return &opts
}

// RefillLowWatermark returns the low watermark for refilling the pool.
func (o *KVArrayPoolOptions) RefillLowWatermark() float64 { return o.refillLowWatermark }

// SetRefillHighWatermark sets the high watermark for refilling the pool.
func (o *KVArrayPoolOptions) SetRefillHighWatermark(v float64) *KVArrayPoolOptions {
	opts := *o
	opts.refillHighWatermark = v
	return &opts
}

// RefillHighWatermark returns the high watermark for stop refilling the pool.
func (o *KVArrayPoolOptions) RefillHighWatermark() float64 { return o.refillHighWatermark }

type kvArrayPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

func newkVArrayPoolMetrics(m tally.Scope) kvArrayPoolMetrics {
	return kvArrayPoolMetrics{
		free:       m.Gauge("free"),
		total:      m.Gauge("total"),
		getOnEmpty: m.Counter("get-on-empty"),
		putOnFull:  m.Counter("put-on-full"),
	}
}

// KVArrayPool is a value pool.
type KVArrayPool struct {
	values              chan KVArray
	alloc               func() KVArray
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
	dice                int32
	metrics             kvArrayPoolMetrics
}

// NewKVArrayPool creates a new pool.
func NewKVArrayPool(opts *KVArrayPoolOptions) *KVArrayPool {
	if opts == nil {
		opts = NewKVArrayPoolOptions()
	}

	p := &KVArrayPool{
		values: make(chan KVArray, opts.Size()),
		size:   opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: newkVArrayPoolMetrics(opts.InstrumentOptions().MetricsScope()),
	}

	p.setGauges()

	return p
}

// Init initializes the pool.
func (p *KVArrayPool) Init(alloc func() KVArray) {
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
func (p *KVArrayPool) Get() KVArray {
	if atomic.LoadInt32(&p.initialized) != 1 {
		panic(errors.New("get before pool is initialized"))
	}

	var v KVArray
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
func (p *KVArrayPool) Put(v KVArray) {
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

func (p *KVArrayPool) trySetGauges() {
	if atomic.AddInt32(&p.dice, 1)%100 == 0 {
		p.setGauges()
	}
}

func (p *KVArrayPool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *KVArrayPool) tryFill() {
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
