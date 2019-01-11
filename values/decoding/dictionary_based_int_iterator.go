package decoding

import (
	"fmt"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"

	bitstream "github.com/dgryski/go-bitstream"
)

// dictionaryBasedIntIterator iterates through a dict encoded stream of ints.
// NB(xichen): It might be worth bit unpacking the dictionary for faster decoding.
type dictionaryBasedIntIterator struct {
	bitReader           *bitstream.BitReader
	encodedDict         []byte
	minValue            int
	bytesPerDictValue   int
	bitsPerEncodedValue int

	curr int
	err  error
}

func newDictionaryBasedIntIterator(
	reader xio.Reader,
	encodedDict []byte, // Bit-packed encoded int dictionary
	minValue int, // The original value is the sum of minValue and the decoded value
	bytesPerDictValue int, // Number of bytes per dictionary value
	bitsPerEncodedValue int, // Number of bits per encoded value
) *dictionaryBasedIntIterator {
	return &dictionaryBasedIntIterator{
		bitReader:           bitstream.NewReader(reader),
		encodedDict:         encodedDict,
		minValue:            minValue,
		bytesPerDictValue:   bytesPerDictValue,
		bitsPerEncodedValue: bitsPerEncodedValue,
	}
}

// Next iteration.
func (it *dictionaryBasedIntIterator) Next() bool {
	if it.err != nil {
		return false
	}

	// Read the idx into the dict first.
	var dictIdx uint64
	dictIdx, it.err = it.bitReader.ReadBits(it.bitsPerEncodedValue)
	if it.err != nil {
		return false
	}

	start := int(dictIdx) * it.bytesPerDictValue
	if end := start + it.bytesPerDictValue; end > len(it.encodedDict) {
		it.err = fmt.Errorf("int dictionary index %d out of range %d", end, len(it.encodedDict))
		return false
	}
	decodedVal := xio.ReadInt(it.bytesPerDictValue, it.encodedDict[start:])
	it.curr = it.minValue + int(decodedVal)
	return true
}

// Current returns the current int.
func (it *dictionaryBasedIntIterator) Current() int { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *dictionaryBasedIntIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close closes the iterator.
func (it *dictionaryBasedIntIterator) Close() {
	it.bitReader = nil
	it.encodedDict = nil
	it.err = nil
}
