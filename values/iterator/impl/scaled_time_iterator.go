package impl

import (
	"time"

	"github.com/xichen2020/eventdb/values/iterator"
)

type scaleTimeFn func(v int64, resolution time.Duration) int64

// scaledTimeIterator scales the time values to the specified resolution.
type scaledTimeIterator struct {
	resolution  time.Duration
	valuesIt    iterator.ForwardTimeIterator
	scaleTimeFn scaleTimeFn

	done bool
	curr int64
	err  error
}

// NewScaledTimeIterator creates a new scaled time iterator.
func NewScaledTimeIterator(
	valuesIt iterator.ForwardTimeIterator,
	resolution time.Duration,
	scaleTimeFn scaleTimeFn,
) iterator.ForwardTimeIterator {
	return &scaledTimeIterator{
		resolution:  resolution,
		valuesIt:    valuesIt,
		scaleTimeFn: scaleTimeFn,
	}
}

// Next iteration.
func (it *scaledTimeIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	if !it.valuesIt.Next() {
		it.done = true
		it.err = it.valuesIt.Err()
		return false
	}
	// Scale down the current value to the specified resolution.
	it.curr = it.valuesIt.Current()
	return true
}

// Current returns the current int64.
func (it *scaledTimeIterator) Current() int64 { return it.scaleTimeFn(it.curr, it.resolution) }

// Err returns any error recorded while iterating.
func (it *scaledTimeIterator) Err() error { return it.err }

// Close the iterator.
func (it *scaledTimeIterator) Close() {
	it.valuesIt.Close()
	it.valuesIt = nil
	it.scaleTimeFn = nil
	it.err = nil
}
