package digest

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteDigest(t *testing.T) {
	buf := NewBuffer()
	buf.WriteDigest(2)
	require.Equal(t, []byte{0x2, 0x0, 0x0, 0x0}, []byte(buf))
}

func TestWriteDigestToFile(t *testing.T) {
	fd := createTempFile(t)
	filePath := fd.Name()
	defer os.Remove(filePath)

	buf := NewBuffer()
	require.NoError(t, buf.WriteDigestToFile(fd, 20))
	fd.Close()

	fd, err := os.Open(filePath)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(fd)
	require.NoError(t, err)
	fd.Close()
	require.Equal(t, []byte{0x14, 0x0, 0x0, 0x0}, b)
}

func TestReadDigest(t *testing.T) {
	buf := ToBuffer([]byte{0x0, 0x1, 0x0, 0x1, 0x0, 0x1})
	require.Equal(t, uint32(0x1000100), buf.ReadDigest())
}

func TestReadDigestFromFile(t *testing.T) {
	fd := createTempFile(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0xa, 0x0, 0x0, 0x0}
	fd.Write(b)
	fd.Seek(0, 0)

	buf := NewBuffer()
	res, err := buf.ReadDigestFromFile(fd)
	require.NoError(t, err)
	require.Equal(t, uint32(10), res)
}

func createTempFile(t *testing.T) *os.File {
	fd, err := ioutil.TempFile("", "testfile")
	require.NoError(t, err)
	return fd
}
