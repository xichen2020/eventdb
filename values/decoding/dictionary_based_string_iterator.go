package decoding

import (
	"encoding/binary"
	"fmt"
	"io"
)

// dictionaryBasedStringIterator iterates over a
// dict encoded stream of string data.
type dictionaryBasedStringIterator struct {
	reader io.ByteReader
	// extDict is passed externally from the string decoder
	// and should not be mutated during iteration.
	extDict []string

	curr string
	err  error
}

func newDictionaryBasedStringIterator(
	reader io.ByteReader,
	extDict []string,
) *dictionaryBasedStringIterator {
	return &dictionaryBasedStringIterator{
		reader:  reader,
		extDict: extDict,
	}
}

// Next iteration.
func (it *dictionaryBasedStringIterator) Next() bool {
	// Bail early if dictionary is empty, ie values is also empty.
	if it.err != nil || len(it.extDict) == 0 {
		return false
	}

	var idx int64
	idx, it.err = binary.ReadVarint(it.reader)
	if it.err != nil {
		return false
	}
	if int(idx) >= len(it.extDict) {
		it.err = fmt.Errorf("string dictionary index %d out of range %d", idx, len(it.extDict))
		return false
	}

	it.curr = it.extDict[idx]
	return true
}

// Current returns the current string.
func (it *dictionaryBasedStringIterator) Current() string { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *dictionaryBasedStringIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close the iterator.
func (it *dictionaryBasedStringIterator) Close() {
	it.extDict = nil
	it.err = nil
	it.reader = nil
}
