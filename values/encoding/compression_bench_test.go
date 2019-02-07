package encoding

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/gozstd"
)

const (
	testDataFilePath       = "./testdata/testdata.json"
	compressedDataFilePath = "../decoding/testdata/testdata.json.compressed"
)

func compressData(data []byte) error {
	out, err := os.OpenFile(compressedDataFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer out.Close()

	writer := gozstd.NewWriter(out)
	_, err = writer.Write(data)
	defer writer.Close()
	defer writer.Release()
	return err
}

// Benchmark compression results:
// Data: 279 MB rtapi logs (100k log lines)
// Compression speed: ~ 250 MB / s
// Output: 36 MB compressed
//
func BenchmarkCompression(b *testing.B) {
	data, err := ioutil.ReadFile(testDataFilePath)
	require.NoError(b, err)

	for n := 0; n < b.N; n++ {
		err = compressData(data)
		require.NoError(b, err)
	}
}
