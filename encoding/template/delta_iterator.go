// add build ignore flag to ignore arithmetic ops against iface.
// +build ignore

package template

import (
	bitstream "github.com/dgryski/go-bitstream"
	"github.com/mauricelam/genny/generic"
)

// GenericValue represents a generic value.
type GenericValue interface {
	generic.Type
}

// DeltaValueIterator iterates over a stream of delta encoded data.
type DeltaValueIterator struct {
	bitReader           *bitstream.BitReader
	bitsPerEncodedValue int64
	negativeBit         uint64
	curr                GenericValue
	err                 error
	closed              bool
}

func newDeltaValueIterator(
	extBitReader *bitstream.BitReader, // bitReader is an external bit reader for re-use.
	bitsPerEncodedValue int64,
	deltaStart GenericValue,
) *DeltaValueIterator {
	return &DeltaValueIterator{
		bitReader:           extBitReader,
		bitsPerEncodedValue: bitsPerEncodedValue,
		negativeBit:         1 << uint(bitsPerEncodedValue-1),
		curr:                deltaStart,
	}
}

// Next iteration.
func (it *DeltaValueIterator) Next() bool {
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
		it.curr -= GenericValue(delta)
	} else {
		it.curr += GenericValue(delta)
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
	return nil
}
