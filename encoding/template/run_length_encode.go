package template

import (
	"encoding/binary"
	"io"

	"github.com/mauricelam/genny/generic"
	"github.com/xichen2020/eventdb/x/bytes"
)

// GenericValue is a generic type.
type GenericValue generic.Type

// ReadValueFn reads a GenericValue from an `io.Reader`.
type ReadValueFn func(writer io.Writer, value GenericValue) error

// ForwardValueIterator allows iterating over a stream of GenericValue.
type ForwardValueIterator interface {
	generic.Type

	io.Closer
	Next() bool
	Err() error
	Current() GenericValue
}

// runLengthEncodeValue run length encodes a stream of GenericValue.
func runLengthEncodeValue(
	writer io.Writer,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	readValue ReadValueFn,
	valuesIt ForwardValueIterator,
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
		n := binary.PutVarint(*extBuf, int64(repetitions))
		if _, err := writer.Write((*extBuf)[:n]); err != nil {
			return err
		}
		if err := readValue(writer, last); err != nil {
			return err
		}
		last = curr
		repetitions = 1
	}
	if err := valuesIt.Err(); err != nil {
		return err
	}

	// Encode the final value.
	n := binary.PutVarint(*extBuf, int64(repetitions))
	if _, err := writer.Write((*extBuf)[:n]); err != nil {
		return err
	}
	if err := readValue(writer, last); err != nil {
		return err
	}
	return nil
}
