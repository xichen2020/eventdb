package template

import (
	"encoding/binary"
	"io"

	"github.com/xichen2020/eventdb/x/bytes"
)

// writeValueFn writes a value to the writer.
type writeValueFn func(writer io.Writer, value GenericValue) error

// runLengthEncodeValue run length encodes a stream of values.
func runLengthEncodeValue(
	valuesIt ForwardValueIterator,
	writeValueFn writeValueFn,
	writer io.Writer,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
) error {
	// Ensure that our buffer size is large enough to handle varint ops.
	*extBuf = bytes.EnsureBufferSize(*extBuf, binary.MaxVarintLen64, bytes.DontCopyData)

	var (
		firstTime   = true
		last        GenericValue
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
		if err := writeRLE(last, repetitions, writeValueFn, writer, *extBuf); err != nil {
			return err
		}
		last = curr
		repetitions = 1
	}
	if err := valuesIt.Err(); err != nil {
		return err
	}

	return writeRLE(last, repetitions, writeValueFn, writer, *extBuf)
}

func writeRLE(
	value GenericValue,
	repetitions int,
	writeValueFn writeValueFn,
	writer io.Writer,
	extBuf []byte, // extBuf is an external byte buffer for memory re-use.
) error {
	n := binary.PutVarint(extBuf, int64(repetitions))
	if _, err := writer.Write(extBuf[:n]); err != nil {
		return err
	}
	return writeValueFn(writer, value)
}
