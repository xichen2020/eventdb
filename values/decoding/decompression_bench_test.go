package decoding

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/gozstd"
)

const (
	compressedDataFilePath = "./testdata/testdata.json.compressed"
	// About the average size of each log message.
	decompressBufferSize = 1300
)

func decompressData(data []byte) error {
	reader := gozstd.NewReader(bytes.NewReader(data))
	defer reader.Release()
	buf := make([]byte, decompressBufferSize)
	var err error
	for err != io.EOF {
		_, err = reader.Read(buf)
	}
	return err
}

// Benchmark decompression results:
// Data: 36 MB compressed rtapi logs (100k log lines)
// Decompression speed: ~ 1 GB / s
// Output: 279 MB uncompressed
//
func BenchmarkDecompression(b *testing.B) {
	data, err := ioutil.ReadFile(compressedDataFilePath)
	require.NoError(b, err)

	for n := 0; n < b.N; n++ {
		err = decompressData(data)
		// io.EOF is expected.
		if err == io.EOF {
			continue
		}
		require.NoError(b, err)
	}
}
