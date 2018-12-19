package template

import (
	"encoding/binary"
	"errors"

	"github.com/xichen2020/eventdb/x/io"
)

var (
	errInvalidNumberOfRepetitions = errors.New("invalid # of repetitions < 1")
)

// ReadValueFn reads a GenericValue from an `io.Reader`.
type ReadValueFn func(reader io.Reader) (GenericValue, error)

// runLengthDecodeValue run length decodes a stream of GenericValues.
func runLengthDecodeValue(
	reader io.Reader,
	ReadValue ReadValueFn,
) *RunLengthValueIterator {
	return newRunLengthValueIterator(reader, ReadValue)
}

// RunLengthValueIterator iterates over a run length encoded stream of GenericValue data.
type RunLengthValueIterator struct {
	reader      io.Reader
	readValue   ReadValueFn
	curr        GenericValue
	repetitions int64
	closed      bool
	err         error
}

// Next iteration.
func (rl *RunLengthValueIterator) Next() bool {
	if rl.closed || rl.err != nil {
		return false
	}

	if rl.repetitions == 0 {
		rl.repetitions, rl.err = binary.ReadVarint(rl.reader)
		if rl.err != nil {
			return false
		}
		rl.curr, rl.err = rl.readValue(rl.reader)
	}

	if rl.repetitions < 1 {
		rl.err = errInvalidNumberOfRepetitions
		return false
	}

	if rl.repetitions >= 1 {
		rl.repetitions--
	}

	return true
}

// Current returns the current string.
func (rl *RunLengthValueIterator) Current() GenericValue { return rl.curr }

// Err returns any error recorded while iterating.
func (rl *RunLengthValueIterator) Err() error { return rl.err }

// Close the iterator.
func (rl *RunLengthValueIterator) Close() error {
	rl.closed = true
	rl.err = nil
	rl.reader = nil
	return nil
}

func newRunLengthValueIterator(
	reader io.Reader,
	readValue ReadValueFn,
) *RunLengthValueIterator {
	return &RunLengthValueIterator{
		reader:    reader,
		readValue: readValue,
	}
}
