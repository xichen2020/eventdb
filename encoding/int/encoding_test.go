package int

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	values := NewMockIterator(ctrl)
	encodedData := []byte("fake encoded data")

	encoder := NewMockEncoder(ctrl)
	encoder.EXPECT().Encode(values).Return(nil).Times(1)
	encoder.EXPECT().Bytes().Return(encodedData).Times(1)

	// Encode the data first
	err := encoder.Encode(values)
	require.Nil(t, err)

	// Get the bytes and write to disk.
	f, err := ioutil.TempFile("/tmp", "encodeddata")
	require.Nil(t, err)
	defer os.Remove(f.Name())
	_, err = f.Write(encoder.Bytes())
	require.Nil(t, err)
	err = f.Close()
	require.Nil(t, err)
}

func TestDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := []int{1, 2, 3, 4, 5, 6, 7, 8}
	encodedData := bytes.NewBuffer([]byte{1, 2, 3, 4, 5, 6, 7, 8})

	iter := NewMockIterator(ctrl)
	for _, n := range data {
		iter.EXPECT().Current().Return(n).Times(1)
	}
	iter.EXPECT().Next().Return(true).Times(len(data))
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)

	decoder := NewMockDecoder(ctrl)
	decoder.EXPECT().Decode(encodedData).Return(iter, nil).Times(1)

	// Decode the encoded data.
	values, err := decoder.Decode(encodedData)
	require.Nil(t, err)

	// Iterate over decoded values.
	idx := 0
	for values.Next() {
		value := iter.Current()
		require.Equal(t, data[idx], value)
		idx++
	}
	// Check for error after iteration is finished.
	require.Nil(t, iter.Err())
}
