package encoding

import (
	"encoding/binary"
	"errors"

	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/unsafe"
)

// Errors.
var (
	errCannotSeekWithoutBlockStorage               = errors.New("cannot seek without underlying block storage")
	errFailedToCastReaderToSkippableCompressReader = errors.New("failed to cast reader to skippable compress reader")
)

// RawSizeStringIteratorOptions informs how to decode string data.
type RawSizeStringIteratorOptions struct {
	UseBlocks bool
}

// RawSizeStringIterator iterates over a stream of
// raw size encoded string data.
type RawSizeStringIterator struct {
	reader io.Reader
	extBuf *[]byte
	opts   RawSizeStringIteratorOptions
	curr   string
	// numOfEventsRead is the # of events read so far in a block.
	numOfEventsRead int
	err             error
	closed          bool
}

func newRawSizeStringIterator(
	reader io.Reader,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	opts RawSizeStringIteratorOptions,
) *RawSizeStringIterator {
	return &RawSizeStringIterator{
		reader: reader,
		extBuf: extBuf,
		opts:   opts,
	}
}

// Next iteration.
func (it *RawSizeStringIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	return it.tryRead()
}

// Seek to an offset (in # of events) relative to either the current event.
func (it *RawSizeStringIterator) Seek(offset int) error {
	// Seek requires the underlying reader to be a `skippableCompressReader`.
	if !it.opts.UseBlocks {
		return errCannotSeekWithoutBlockStorage
	}
	reader, ok := it.reader.(*skippableCompressReader)
	if !ok {
		return errFailedToCastReaderToSkippableCompressReader
	}

	// Calculate if we need to skip blocks or not.
	remaining := reader.numOfEvents() - it.numOfEventsRead
	// Skip until we can seek to an event w/in a block.
	for offset > remaining {
		// Move the offset along.
		offset -= remaining
		if err := reader.skip(); err != nil {
			return err
		}
		// Since we just skipped a block, remaining is just the total # of events in this block.
		remaining = reader.numOfEvents()
	}

	// Now the offset is w/in the current block.
	// Iterate until we reach the event.
	for offset >= 0 {
		if !it.Next() {
			return it.err
		}
		offset--
	}

	return nil
}

// Current returns the current string.
// NB(bodu): Caller must copy the current string to have a valid reference btwn `Next()` calls.
func (it *RawSizeStringIterator) Current() string { return it.curr }

// Err returns any error recorded while iterating.
func (it *RawSizeStringIterator) Err() error { return it.err }

// Close the iterator.
func (it *RawSizeStringIterator) Close() error {
	it.closed = true
	it.extBuf = nil
	it.err = nil
	it.reader = nil
	return nil
}

// tryRead tries a read and handles retries if we reach the end of a block.
func (it *RawSizeStringIterator) tryRead() bool {
	var rawSizeBytes int64
	rawSizeBytes, it.err = binary.ReadVarint(it.reader)
	// Skip to the next block if we reach the end of one and retry read.
	if it.err == errEndOfBlock {
		return it.skip()
	}
	if it.err != nil {
		return false
	}

	*it.extBuf = bytes.EnsureBufferSize(*it.extBuf, int(rawSizeBytes), bytes.DontCopyData)

	_, it.err = it.reader.Read((*it.extBuf)[:rawSizeBytes])
	// Skip to the next block if we reach the end of one and retry read.
	if it.err == errEndOfBlock {
		return it.skip()
	}
	if it.err != nil {
		return false
	}

	it.curr = unsafe.ToString((*it.extBuf)[:rawSizeBytes])
	it.numOfEventsRead++
	return true
}

// skip to the next block and do some housekeeping.
func (it *RawSizeStringIterator) skip() bool {
	// Seek requires the underlying reader to be a `io.SkippableReader`.
	if !it.opts.UseBlocks {
		it.err = errCannotSeekWithoutBlockStorage
		return false
	}
	reader, ok := it.reader.(*skippableCompressReader)
	if !ok {
		it.err = errFailedToCastReaderToSkippableCompressReader
		return false
	}

	it.err = reader.skip()
	if it.err != nil {
		return false
	}
	// clear the error otherwise.
	it.err = nil
	it.numOfEventsRead = 0
	return it.tryRead()
}
