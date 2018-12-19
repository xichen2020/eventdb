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

	"io"

	"github.com/xichen2020/eventdb/x/bytes"
)

// WriteValueFn reads a bool from an `io.Reader`.
type WriteValueFn func(writer io.Writer, value bool) error

// ForwardBoolIterator allows iterating over a stream of bool.

// runLengthEncodeBool run length encodes a stream of bool.
func runLengthEncodeBool(
	writer io.Writer,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	writeValue WriteValueFn,
	valuesIt ForwardBoolIterator,
) error {
	// Ensure that our buffer size is large enough to handle varint ops.
	*extBuf = bytes.EnsureBufferSize(*extBuf, binary.MaxVarintLen64, bytes.DontCopyData)

	var (
		firstTime   = true
		last        bool
		repetitions = 1
	)
	for valuesIt.Next() {
		curr := valuesIt.Current()
		// Set on the first value.
		if firstTime {
			last = curr
			firstTime = false
			continue
		}

		// Incrememnt repetitions and continue if we find a repetition.
		if last == curr {
			repetitions++
			continue
		}

		// last and curr don't match, write out the run length encoded repetitions
		// and perform housekeeping.
		writeRLE(writer, extBuf, writeValue, last, int64(repetitions))
		last = curr
		repetitions = 1
	}
	if err := valuesIt.Err(); err != nil {
		return err
	}

	writeRLE(writer, extBuf, writeValue, last, int64(repetitions))

	return nil
}

func writeRLE(
	writer io.Writer,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	writeValue WriteValueFn,
	value bool,
	repetitions int64,
) error {
	// Encode the final value.
	n := binary.PutVarint(*extBuf, int64(repetitions))
	if _, err := writer.Write((*extBuf)[:n]); err != nil {
		return err
	}
	return writeValue(writer, value)
}
