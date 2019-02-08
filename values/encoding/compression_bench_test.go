package encoding

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/gozstd"
)

const (
	testDataFilePath = "./testdata/testdata.json"
)

func compressData(data []byte) error {
	out, err := ioutil.TempFile("/tmp", "*")
	if err != nil {
		return err
	}
	defer os.Remove(out.Name())

	writer := gozstd.NewWriter(out)
	_, err = writer.Write(data)
	if err != nil {
		return err
	}
	defer writer.Release()
	return writer.Close()
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
