package template

import (
	"encoding/binary"
	"io"

	"github.com/mauricelam/genny/generic"
	"github.com/xichen2020/eventdb/x/bytes"
)

// GenericValue is a generic type.
type GenericValue generic.Type

// ValueMarshalFn reads a GenericValue from an `io.Reader`.
type ValueMarshalFn func(writer io.Writer, value GenericValue) error

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
	marshalFn ValueMarshalFn,
	valuesIt ForwardValueIterator,
) error {
	// Ensure that our buffer size is large enough to handle varint ops.
	*extBuf = bytes.EnsureBufferSize(*extBuf, binary.MaxVarintLen64, bytes.DontCopyData)

	var (
		last  *GenericValue
		count int
	)
	for valuesIt.Next() {
		curr := valuesIt.Current()
		// Set on the first value.
		if last == nil {
			last = &curr
			continue
		}

		// Incrememnt count and continue if we find a repeition.
		if *last == curr {
			count++
			continue
		}

		// last and curr don't match, write out the run length encoded repetitions
		// and perform housekeeping.
		n := binary.PutVarint(*extBuf, int64(count))
		if _, err := writer.Write((*extBuf)[:n]); err != nil {
			return err
		}
		if err := marshalFn(writer, *last); err != nil {
			return err
		}
		*last = curr
		count = 0
	}
	if err := valuesIt.Err(); err != nil {
		return err
	}

	// Encode the final value.
	n := binary.PutVarint(*extBuf, int64(count))
	if _, err := writer.Write((*extBuf)[:n]); err != nil {
		return err
	}
	if err := marshalFn(writer, *last); err != nil {
		return err
	}
	return nil
}
