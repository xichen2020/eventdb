package template

import (
	"io"

	xio "github.com/xichen2020/eventdb/x/io"

	bitstream "github.com/dgryski/go-bitstream"
)

type applyOpToValueIntFn func(v GenericValue, delta int) GenericValue

// deltaValueIterator iterates over a stream of delta encoded data.
type deltaValueIterator struct {
	bitReader           *bitstream.BitReader
	bitsPerEncodedValue int64
	addFn               applyOpToValueIntFn
	negativeBit         uint64

	closed       bool
	curr         GenericValue
	err          error
	isDeltaValue bool
}

func newDeltaValueIterator(
	reader xio.Reader,
	bitsPerEncodedValue int64, // This includes the sign bit
	addFn applyOpToValueIntFn,
) *deltaValueIterator {
	return &deltaValueIterator{
		bitReader:           bitstream.NewReader(reader),
		bitsPerEncodedValue: bitsPerEncodedValue,
		addFn:               addFn,
		negativeBit:         1 << uint(bitsPerEncodedValue-1),
	}
}

// Next returns true if there are more values to be iterated over.
func (it *deltaValueIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	// First value is special and written as 64 bits.
	if !it.isDeltaValue {
		var firstValue uint64
		// The first value is encoded in full 64 bit.
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
	intDelta := int(delta)
	isNegative := (delta & it.negativeBit) == it.negativeBit
	if isNegative {
		// Zero out the negative bit.
		delta &^= it.negativeBit
		intDelta = -int(delta)
	}
	it.curr = it.addFn(it.curr, intDelta)
	return true
}

// Current returns the current GenericValue.
func (it *deltaValueIterator) Current() GenericValue { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *deltaValueIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close closes the iterator.
func (it *deltaValueIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.bitReader = nil
	it.err = nil
	it.addFn = nil
	return nil
}
