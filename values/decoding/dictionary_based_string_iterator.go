package decoding

import (
	"encoding/binary"
	"fmt"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"
)

// dictionaryBasedStringIterator iterates over a
// dict encoded stream of string data.
type dictionaryBasedStringIterator struct {
	reader xio.Reader
	// extDict is passed externally from the string encoder
	// and should not be mutated during iteration.
	extDict []string

	curr string
	err  error
}

func newDictionaryBasedStringIterator(
	reader xio.Reader,
	extDict []string,
) *dictionaryBasedStringIterator {
	return &dictionaryBasedStringIterator{
		reader:  reader,
		extDict: extDict,
	}
}

// Next iteration.
func (it *dictionaryBasedStringIterator) Next() bool {
	if it.err != nil {
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
