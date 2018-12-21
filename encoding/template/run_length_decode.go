package template

import (
	"encoding/binary"
	"errors"

	"github.com/xichen2020/eventdb/x/io"
)

var (
	errNonPositiveNumberOfRepetitions = errors.New("non positive number of repetitions")
)

// readValueFn reads a GenericRLValue from an `io.Reader`.
type readValueFn func(reader io.Reader) (GenericRLValue, error)

// runLengthDecodeValue run length decodes a stream of GenericRLValues.
func runLengthDecodeValue(
	reader io.Reader,
	readValueFn readValueFn,
) *RunLengthValueIterator {
	return newValueIterator(reader, readValueFn)
}

// RunLengthValueIterator iterates over a run length encoded stream of GenericRLValue data.
type RunLengthValueIterator struct {
	reader      io.Reader
	readValueFn readValueFn
	curr        GenericRLValue
	repetitions int64
	closed      bool
	err         error
}

// Next iteration.
func (rl *RunLengthValueIterator) Next() bool {
	if rl.closed || rl.err != nil {
		return false
	}

	if rl.repetitions > 0 {
		rl.repetitions--
		return true
	}

	rl.repetitions, rl.err = binary.ReadVarint(rl.reader)
	if rl.err != nil {
		return false
	}
	if rl.repetitions < 1 {
		rl.err = errNonPositiveNumberOfRepetitions
		return false
	}

	rl.curr, rl.err = rl.readValueFn(rl.reader)
	if rl.err != nil {
		return false
	}
	rl.repetitions--
	return true
}

// Current returns the current string.
func (rl *RunLengthValueIterator) Current() GenericRLValue { return rl.curr }

// Err returns any error recorded while iterating.
func (rl *RunLengthValueIterator) Err() error { return rl.err }

// Close the iterator.
func (rl *RunLengthValueIterator) Close() error {
	rl.closed = true
	rl.err = nil
	rl.reader = nil
	rl.readValueFn = nil
	return nil
}

func newValueIterator(
	reader io.Reader,
	readValueFn readValueFn,
) *RunLengthValueIterator {
	return &RunLengthValueIterator{
		reader:      reader,
		readValueFn: readValueFn,
	}
}
