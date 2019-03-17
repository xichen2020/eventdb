package decoding

import (
	"fmt"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"

	bitstream "github.com/dgryski/go-bitstream"
)

// dictionaryBasedIntIterator iterates through a dict encoded stream of ints.
type dictionaryBasedIntIterator struct {
	bitReader *bitstream.BitReader

	// extDict is passed externally from the int decoder
	// and should not be mutated during iteration.
	extDict             []int
	bitsPerEncodedValue int
	numEncodedValues    int

	curr  int
	count int
	err   error
}

func newDictionaryBasedIntIterator(
	reader xio.Reader,
	extDict []int, // Decoded int dictionary
	bitsPerEncodedValue int, // Number of bits per encoded value
	numEncodedValues int, // Number of encoded values
) *dictionaryBasedIntIterator {
	return &dictionaryBasedIntIterator{
		bitReader:           bitstream.NewReader(reader),
		extDict:             extDict,
		bitsPerEncodedValue: bitsPerEncodedValue,
		numEncodedValues:    numEncodedValues,
	}
}

// Next iteration.
func (it *dictionaryBasedIntIterator) Next() bool {
	// Bail early if dictionary is empty, ie values is also empty.
	if it.err != nil || len(it.extDict) == 0 || it.count >= it.numEncodedValues {
		return false
	}

	// Read the idx into the dict first.
	var dictIdx uint64
	dictIdx, it.err = it.bitReader.ReadBits(it.bitsPerEncodedValue)
	if it.err != nil {
		return false
	}
	if int(dictIdx) >= len(it.extDict) {
		it.err = fmt.Errorf("int dictionary index %d out of range %d", dictIdx, len(it.extDict))
		return false
	}
	it.curr = it.extDict[dictIdx]
	it.count++
	return true
}

// Current returns the current int.
func (it *dictionaryBasedIntIterator) Current() int { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *dictionaryBasedIntIterator) Err() error {
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
func (it *dictionaryBasedIntIterator) Close() {
	it.bitReader = nil
	it.extDict = nil
	it.err = nil
}
