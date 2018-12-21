package template

import (
	bitstream "github.com/dgryski/go-bitstream"
)

func encodeDeltaValue(
	bitWriter *bitstream.BitWriter,
	bitsPerEncodedValue int64,
	valuesIt ForwardValueIterator,
	subFn func(curr GenericValue, last GenericValue) int,
	asUint64Fn func(v GenericValue) uint64,
) error {
	// Encode the first value which is always a delta of 0.
	if !valuesIt.Next() {
		return valuesIt.Err()
	}

	firstValue := valuesIt.Current()
	if err := bitWriter.WriteBits(asUint64Fn(firstValue), uint64NumBits); err != nil {
		return err
	}

	negativeBit := int(1 << uint(bitsPerEncodedValue-1))
	// Set last to be the first value and start iterating.
	last := firstValue
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
