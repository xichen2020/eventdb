package template

import (
	"io"

	bitstream "github.com/dgryski/go-bitstream"
	"github.com/mauricelam/genny/generic"
)

// GenericValue represents a generic value type.
type GenericValue generic.Type

// RewindableValueIterator allows iterating over a stream of GenericValue.
type RewindableValueIterator interface {
	generic.Type
	io.Closer
	Next() bool
	Err() error
	Current() GenericValue
	Rewind()
}

func encodeDeltaValue(
	bitWriter *bitstream.BitWriter,
	bitsPerEncodedValue int64,
	valuesIt RewindableValueIterator,
	subFn func(curr GenericValue, last GenericValue) int,
) error {
	// Encode the first value which is always a delta of 0.
	if !valuesIt.Next() {
		return valuesIt.Err()
	}

	// Write an extra bit to encode the sign of the delta.
	if err := bitWriter.WriteBits(uint64(0), int(bitsPerEncodedValue)); err != nil {
		return err
	}

	negativeBit := int(1 << uint(bitsPerEncodedValue-1))
	// Set last to be the first value and start iterating.
	last := valuesIt.Current()
	for valuesIt.Next() {
		curr := valuesIt.Current()
		delta := subFn(curr, last)
		if delta < 0 {
			// Flip the sign.
			delta = -delta
			// Set the MSB if the sign is negative.
			delta |= negativeBit
		}
		if err := bitWriter.WriteBits(uint64(delta), int(bitsPerEncodedValue)); err != nil {
			return err
		}
		// Housekeeping.
		last = curr
	}

	return valuesIt.Err()
}
