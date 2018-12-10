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
func (d *DeltaIntIterator) Next() bool {
	if d.closed || d.err != nil {
		return false
	}

	// Read in an extra bit for the sign.
	var delta uint64
	delta, d.err = d.bitReader.ReadBits(int(d.bitsPerEncodedValue))
	if d.err != nil {
		return false
	}
	// Check if negative bit is set.
	isNegative := (delta & d.negativeBit) == d.negativeBit
	if isNegative {
		// Zero out the negative bit.
		delta &^= d.negativeBit
		d.curr -= int(delta)
	} else {
		d.curr += int(delta)
	}

	return true
}

// Current returns the current int.
func (d *DeltaIntIterator) Current() int { return d.curr }

// Err returns any error recorded while iterating.
func (d *DeltaIntIterator) Err() error { return d.err }

// Close the iterator.
func (d *DeltaIntIterator) Close() error {
	d.closed = true
	d.bitReader = nil
	return nil
}
