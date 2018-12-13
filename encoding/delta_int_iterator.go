package encoding

import (
	bitstream "github.com/dgryski/go-bitstream"
)

// DeltaIntIterator iterates over a stream of delta encoded data.
type DeltaIntIterator struct {
	bitReader           *bitstream.BitReader
	bitsPerEncodedValue int64
	negativeBit         uint64
	curr                int
	err                 error
	closed              bool
}

func newDeltaIntIterator(
	extBitReader *bitstream.BitReader, // bitReader is an external bit reader for re-use.
	bitsPerEncodedValue int64,
	deltaStart int64,
) *DeltaIntIterator {
	return &DeltaIntIterator{
		bitReader:           extBitReader,
		bitsPerEncodedValue: bitsPerEncodedValue,
		negativeBit:         1 << uint(bitsPerEncodedValue-1),
		curr:                int(deltaStart),
	}
}

// Next iteration.
func (it *DeltaIntIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
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
		it.curr -= int(delta)
	} else {
		it.curr += int(delta)
	}

	return true
}

// Current returns the current int.
func (it *DeltaIntIterator) Current() int { return it.curr }

// Err returns any error recorded while iterating.
func (it *DeltaIntIterator) Err() error { return it.err }

// Close the iterator.
func (it *DeltaIntIterator) Close() error {
	it.closed = true
	it.bitReader = nil
	it.err = nil
	return nil
}
