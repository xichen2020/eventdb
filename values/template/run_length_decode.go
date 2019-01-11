package template

import (
	"encoding/binary"
	"errors"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"
)

var (
	errNonPositiveNumberOfRepetitions = errors.New("non positive number of repetitions")
)

// readValueFn reads a value from reader.
type readValueFn func(reader xio.Reader) (GenericValue, error)

// runLengthDecodeValue run length decodes a stream of GenericValues.
func runLengthDecodeValue(
	readValueFn readValueFn,
	reader xio.Reader,
) *runLengthValueIterator {
	return newRunLengthValueIterator(reader, readValueFn)
}

// runLengthValueIterator iterates over a run length encoded stream of values.
type runLengthValueIterator struct {
	reader      xio.Reader
	readValueFn readValueFn

	curr        GenericValue
	repetitions int64
	err         error
}

// Next returns true if there are more values to be iterated over, and false otherwise.
func (it *runLengthValueIterator) Next() bool {
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

// Current returns the current string.
func (it *runLengthValueIterator) Current() GenericValue { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *runLengthValueIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close the iterator.
func (it *runLengthValueIterator) Close() {
	it.err = nil
	it.reader = nil
	it.readValueFn = nil
}

func newRunLengthValueIterator(
	reader xio.Reader,
	readValueFn readValueFn,
) *runLengthValueIterator {
	return &runLengthValueIterator{
		reader:      reader,
		readValueFn: readValueFn,
	}
}
