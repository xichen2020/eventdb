// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package decoding

import (
	"encoding/binary"

	"errors"

	"io"
)

var (
	errNonPositiveNumberOfRepetitions = errors.New("non positive number of repetitions")
)

// readValueFn reads a value from reader.
type readValueFn func(reader io.ByteReader) (bool, error)

// runLengthDecodeBool run length decodes a stream of Bools.
func runLengthDecodeBool(
	readValueFn readValueFn,
	reader io.ByteReader,
) *runLengthBoolIterator {
	return newRunLengthBoolIterator(reader, readValueFn)
}

// runLengthBoolIterator iterates over a run length encoded stream of values.
type runLengthBoolIterator struct {
	reader      io.ByteReader
	readValueFn readValueFn

	curr        bool
	repetitions int64
	err         error
}

// Next returns true if there are more values to be iterated over, and false otherwise.
func (it *runLengthBoolIterator) Next() bool {
	if it.err != nil {
		return false
	}

	if it.repetitions > 0 {
		it.repetitions--
		return true
	}

	it.repetitions, it.err = binary.ReadVarint(it.reader)
	if it.err != nil {
		return false
	}
	if it.repetitions < 1 {
		it.err = errNonPositiveNumberOfRepetitions
		return false
	}

	it.curr, it.err = it.readValueFn(it.reader)
	if it.err != nil {
		return false
	}
	it.repetitions--
	return true
}

// Current returns the current value.
func (it *runLengthBoolIterator) Current() bool { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *runLengthBoolIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close the iterator.
func (it *runLengthBoolIterator) Close() {
	it.err = nil
	it.reader = nil
	it.readValueFn = nil
}

func newRunLengthBoolIterator(
	reader io.ByteReader,
	readValueFn readValueFn,
) *runLengthBoolIterator {
	return &runLengthBoolIterator{
		reader:      reader,
		readValueFn: readValueFn,
	}
}
