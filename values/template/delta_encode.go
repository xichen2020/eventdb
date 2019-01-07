package template

import (
	bitstream "github.com/dgryski/go-bitstream"
)

func deltaValueEncode(
	valuesIt ForwardValueIterator,
	bitWriter *bitstream.BitWriter,
	bitsPerEncodedValue int, // This includes the sign bit
	subFn func(curr GenericValue, last GenericValue) int,
	asUint64Fn func(v GenericValue) uint64,
) error {
	if !valuesIt.Next() {
		return valuesIt.Err()
	}

	firstValue := valuesIt.Current()
	// Need 64 bits to write out uint64 values.
	if err := bitWriter.WriteBits(asUint64Fn(firstValue), 64); err != nil {
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
		if err := bitWriter.WriteBits(uint64(delta), bitsPerEncodedValue); err != nil {
			return err
		}
		// Housekeeping.
		last = curr
	}

	if err := valuesIt.Err(); err != nil {
		return err
	}

	// Flush the bit writer and pad it with zero bits if necessary.
	return bitWriter.Flush(bitstream.Zero)
}
