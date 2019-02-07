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

	writer := gozstd.NewWriter(out)
	_, err = writer.Write(data)
	return err
}

func BenchmarkCompression(b *testing.B) {
	data, err := ioutil.ReadFile(testDataFilePath)
	require.NoError(b, err)

	for n := 0; n < b.N; n++ {
		err = compressData(data)
		require.NoError(b, err)
	}
}
