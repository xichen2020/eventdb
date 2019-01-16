// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package encoding

import "github.com/xichen2020/eventdb/values/iterator"
import (
	bitstream "github.com/dgryski/go-bitstream"
)

func deltaIntEncode(
	valuesIt iterator.ForwardIntIterator,
	bitWriter *bitstream.BitWriter,
	bitsPerEncodedValue int, // This includes the sign bit
	subFn func(curr int, last int) int,
	asUint64Fn func(v int) uint64,
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
