// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package encoding

import (
	"encoding/binary"

	"errors"

	"github.com/xichen2020/eventdb/x/io"
)

var (
	errNonPositiveNumberOfRepetitions = errors.New("non positive number of repetitions")
)

// readValueFn reads a bool from an `io.Reader`.
type readValueFn func(reader io.Reader) (bool, error)

// runLengthDecodeBool run length decodes a stream of Bools.
func runLengthDecodeBool(
	reader io.Reader,
	readValueFn readValueFn,
) *RunLengthBoolIterator {
	return newRunLengthBoolIterator(reader, readValueFn)
}

// RunLengthBoolIterator iterates over a run length encoded stream of bool data.
type RunLengthBoolIterator struct {
	reader      io.Reader
	readValueFn readValueFn
	curr        bool
	repetitions int64
	closed      bool
	err         error
}

// Next iteration.
func (rl *RunLengthBoolIterator) Next() bool {
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
	rl.repetitions--
	return true
}

// Current returns the current string.
func (rl *RunLengthBoolIterator) Current() bool { return rl.curr }

// Err returns any error recorded while iterating.
func (rl *RunLengthBoolIterator) Err() error { return rl.err }

// Close the iterator.
func (rl *RunLengthBoolIterator) Close() error {
	rl.closed = true
	rl.err = nil
	rl.reader = nil
	rl.readValueFn = nil
	return nil
}

func newRunLengthBoolIterator(
	reader io.Reader,
	readValueFn readValueFn,
) *RunLengthBoolIterator {
	return &RunLengthBoolIterator{
		reader:      reader,
		readValueFn: readValueFn,
	}
}
