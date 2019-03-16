package decoding

import (
	"fmt"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"

	bitstream "github.com/dgryski/go-bitstream"
)

// bitStreamIntIterator iterates through a bit encoded stream of ints.
type bitStreamIntIterator struct {
	bitReader           *bitstream.BitReader
	bitsPerEncodedValue int
	numEncodedValues    int

	curr  int
	count int
	err   error
}

func newBitStreamIntIterator(
	reader xio.Reader,
	bitsPerEncodedValue int, // Number of bits per encoded value
	numEncodedValues int, // Number of encoded values
) *bitStreamIntIterator {
	return &bitStreamIntIterator{
		bitReader:           bitstream.NewReader(reader),
		bitsPerEncodedValue: bitsPerEncodedValue,
		numEncodedValues:    numEncodedValues,
	}
}

// Next iteration.
func (it *bitStreamIntIterator) Next() bool {
	// Bail early if dictionary is empty, ie values is also empty.
	if it.err != nil || it.count >= it.numEncodedValues {
		return false
	}

	// Read the idx into the dict first.
	var val uint64
	val, it.err = it.bitReader.ReadBits(it.bitsPerEncodedValue)
	if it.err != nil {
		return false
	}
	it.curr = int(val)
	it.count++
	return true
}

// Current returns the current int.
func (it *bitStreamIntIterator) Current() int { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *bitStreamIntIterator) Err() error {
	if it.err != io.EOF {
		return it.err
	}
	if it.count == it.numEncodedValues {
		return nil
	}
	it.err = fmt.Errorf("expected %d values but only iterated over %d values", it.numEncodedValues, it.count)
	return it.err
}

// Close closes the iterator.
func (it *bitStreamIntIterator) Close() {
	it.bitReader = nil
	it.err = nil
}
