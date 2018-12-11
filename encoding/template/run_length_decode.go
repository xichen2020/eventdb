package template

import (
	"encoding/binary"

	"github.com/xichen2020/eventdb/x/io"
)

// ValueUnmarshalFn reads a GenericValue from an `io.Reader`.
type ValueUnmarshalFn func(reader io.Reader) (GenericValue, error)

// runLengthDecodeValue run length decodes a stream of GenericValues.
func runLengthDecodeValue(
	reader io.Reader,
	unmarshalFn ValueUnmarshalFn,
) *RunLengthValueIterator {
	return newRunLengthValueIterator(reader, unmarshalFn)
}

// RunLengthValueIterator iterates over a run length encoded stream of GenericValue data.
type RunLengthValueIterator struct {
	reader      io.Reader
	unmarshalFn ValueUnmarshalFn
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

	if rl.repetitions > 0 {
		rl.repetitions--
		return true
	}

	rl.repetitions, rl.err = binary.ReadVarint(rl.reader)
	if rl.err != nil {
		return false
	}

	rl.curr, rl.err = rl.unmarshalFn(rl.reader)
	return rl.err == nil
}

// Current returns the current string.
func (rl *RunLengthValueIterator) Current() GenericValue { return rl.curr }

// Err returns any error recorded while iterating.
func (rl *RunLengthValueIterator) Err() error { return rl.err }

// Close the iterator.
func (rl *RunLengthValueIterator) Close() error {
	rl.closed = true
	return nil
}

func newRunLengthValueIterator(
	reader io.Reader,
	unmarshalFn ValueUnmarshalFn,
) *RunLengthValueIterator {
	return &RunLengthValueIterator{
		reader:      reader,
		unmarshalFn: unmarshalFn,
	}
}
