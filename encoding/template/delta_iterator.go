package template

import (
	bitstream "github.com/dgryski/go-bitstream"
)

type applyOpToValueIntFn func(v GenericValue, delta int) GenericValue

// DeltaValueIterator iterates over a stream of delta encoded data.
type DeltaValueIterator struct {
	bitReader           *bitstream.BitReader
	bitsPerEncodedValue int64
	subFn               applyOpToValueIntFn
	addFn               applyOpToValueIntFn
	negativeBit         uint64
	curr                GenericValue
	err                 error
	closed              bool
	isDeltaValue        bool
}

func newValueIteratorDelta(
	extBitReader *bitstream.BitReader, // bitReader is an external bit reader for re-use.
	bitsPerEncodedValue int64,
	subFn applyOpToValueIntFn,
	addFn applyOpToValueIntFn,
) *DeltaValueIterator {
	return &DeltaValueIterator{
		bitReader:           extBitReader,
		bitsPerEncodedValue: bitsPerEncodedValue,
		subFn:               subFn,
		addFn:               addFn,
		negativeBit:         1 << uint(bitsPerEncodedValue-1),
	}
}

// Next iteration.
func (it *DeltaValueIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	// First value is special and written as 64 bits.
	if !it.isDeltaValue {
		var firstValue uint64
		// Read 64 bits to read in a uint64 value.
		firstValue, it.err = it.bitReader.ReadBits(64)
		if it.err != nil {
			return false
		}
		it.curr = GenericValue(firstValue)
		// The remaining values are delta values.
		it.isDeltaValue = true
		return true
	}

	// Read in an extra bit for the sign.
	var delta uint64
	delta, it.err = it.bitReader.ReadBits(int(it.bitsPerEncodedValue))
	if it.err != nil {
		return false
	}
	// Check if negative bit is set.
	isNegative := (delta & it.negativeBit) == it.negativeBit
	if isNegative {
		// Zero out the negative bit.
		delta &^= it.negativeBit
		it.curr = it.subFn(it.curr, int(delta))
	} else {
		it.curr = it.addFn(it.curr, int(delta))
	}

	return true
}

// Current returns the current GenericValue.
func (it *DeltaValueIterator) Current() GenericValue { return it.curr }

// Err returns any error recorded while iterating.
func (it *DeltaValueIterator) Err() error { return it.err }

// Close the iterator.
func (it *DeltaValueIterator) Close() error {
	it.closed = true
	it.bitReader = nil
	it.err = nil
	it.subFn = nil
	it.addFn = nil
	return nil
}
