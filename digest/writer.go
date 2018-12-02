package digest

import (
	"bufio"
	"io"
	"os"
)

// FdWithDigestWriter provides a buffered writer for writing to the underlying file.
type FdWithDigestWriter interface {
	fdWithDigest
	io.Writer

	Flush() error
}

// FileWithDigestWriter writes data to a file and computes the data checksum in a
// streaming fashion.
type FileWithDigestWriter struct {
	fdWithDigest

	writer    *bufio.Writer
	digestBuf Buffer
}

// NewFdWithDigestWriter creates a new FdWithDigestWriter.
func NewFdWithDigestWriter(bufferSize int) *FileWithDigestWriter {
	return &FileWithDigestWriter{
		fdWithDigest: newFileWithDigest(),
		writer:       bufio.NewWriterSize(nil, bufferSize),
		digestBuf:    NewBuffer(),
	}
}

// Reset resets the writer.
func (w *FileWithDigestWriter) Reset(fd *os.File) {
	w.fdWithDigest.Reset(fd)
	w.writer.Reset(fd)
}

// Write bytes to the underlying file.
func (w *FileWithDigestWriter) Write(b []byte) (int, error) {
	written, err := w.writer.Write(b)
	if err != nil {
		return 0, err
	}
	if _, err := w.Digest().Write(b); err != nil {
		return 0, err
	}
	return written, nil
}

// Close writes the final checksum to file, flushes what's remaining in
// the buffered writer, and closes the underlying file.
func (w *FileWithDigestWriter) Close() error {
	if err := w.writeDigest(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return w.fdWithDigest.Close()
}

// Flush flushes what's remaining in the buffered writes.
func (w *FileWithDigestWriter) Flush() error {
	return w.writer.Flush()
}

func (w *FileWithDigestWriter) writeDigest() error {
	digest := w.Digest().Sum32()
	w.digestBuf.WriteDigest(digest)
	_, err := w.Write(w.digestBuf)
	return err
}
