package digest

import (
	"hash"
	"hash/adler32"
	"io"
	"os"
)

// fdWithDigest is a container for a file descriptor and the digest for the file contents.
type fdWithDigest interface {
	io.Closer

	// Fd returns the file descriptor.
	Fd() *os.File

	// Digest returns the digest.
	Digest() hash.Hash32

	// Reset resets the file descriptor and the digest.
	Reset(fd *os.File)
}

type fileWithDigest struct {
	fd     *os.File
	digest hash.Hash32
}

func newFileWithDigest() *fileWithDigest {
	return &fileWithDigest{
		digest: adler32.New(),
	}
}

func (fwd *fileWithDigest) Fd() *os.File {
	return fwd.fd
}

func (fwd *fileWithDigest) Digest() hash.Hash32 {
	return fwd.digest
}

func (fwd *fileWithDigest) Reset(fd *os.File) {
	fwd.fd = fd
	fwd.digest.Reset()
}

// Close closes the file descriptor.
func (fwd *fileWithDigest) Close() error {
	if fwd.fd == nil {
		return nil
	}
	err := fwd.fd.Close()
	fwd.fd = nil
	return err
}
