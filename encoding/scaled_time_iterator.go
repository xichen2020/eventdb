package encoding

import "time"

type scaleFn func(v int64, resolution time.Duration) int64

// scaledTimeIterator scales down the time values to the specified resolution.
type scaledTimeIterator struct {
	resolution time.Duration
	valuesIt   ForwardTimeIterator
	scaleFn    scaleFn
	curr       int64
	err        error
	closed     bool
}

func newScaledTimeIterator(
	resolution time.Duration,
	valuesIt ForwardTimeIterator,
	scaleFn scaleFn,
) *scaledTimeIterator {
	return &scaledTimeIterator{
		resolution: resolution,
		valuesIt:   valuesIt,
		scaleFn:    scaleFn,
	}
}

// Next iteration.
func (it *scaledTimeIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if !it.valuesIt.Next() {
		it.err = it.valuesIt.Err()
		return false
	}
	// Scale down the current value to the specified resolution.
	it.curr = it.valuesIt.Current()
	return true
}

// Current returns the current int64.
func (it *scaledTimeIterator) Current() int64 { return it.scaleFn(it.curr, it.resolution) }

// Err returns any error recorded while iterating.
func (it *scaledTimeIterator) Err() error { return it.err }

// Close the iterator.
func (it *scaledTimeIterator) Close() error {
	it.closed = true
	it.valuesIt.Close()
	it.valuesIt = nil
	return nil
}
