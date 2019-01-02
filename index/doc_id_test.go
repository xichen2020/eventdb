package index

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/xichen2020/eventdb/digest"

	"github.com/golang/mock/gomock"
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

func TestWriteBitmapDocIDSetIsPartial(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		typeBuf []byte
		sizeBuf []byte
		dataBuf []byte
	)
	ds := &bitmapBasedDocIDSet{
		numTotalDocs: 4,
		bm:           roaring.NewBitmap(2, 3),
	}
	writer := digest.NewMockFdWithDigestWriter(ctrl)
	gomock.InOrder(
		writer.EXPECT().
			Write(gomock.Any()).
			DoAndReturn(func(data []byte) (int, error) {
				typeBuf = make([]byte, len(data))
				copy(typeBuf, data)
				return 1, nil
			}),
		writer.EXPECT().
			Write(gomock.Any()).
			DoAndReturn(func(data []byte) (int, error) {
				sizeBuf = make([]byte, len(data))
				copy(sizeBuf, data)
				return 1, nil
			}),
		writer.EXPECT().
			Write(gomock.Any()).
			DoAndReturn(func(data []byte) (int, error) {
				dataBuf = make([]byte, len(data))
				copy(dataBuf, data)
				return len(data), nil
			}),
	)

	require.NoError(t, ds.WriteTo(writer, bytes.NewBuffer(nil)))

	docIDSetType, n := binary.Varint(typeBuf)
	require.Equal(t, len(typeBuf), n)
	require.Equal(t, int(bitmapBasedDocIDSetType), int(docIDSetType))

	size, n := binary.Varint(sizeBuf)
	require.Equal(t, len(sizeBuf), n)
	require.Equal(t, len(dataBuf), int(size))

	b := roaring.NewBitmap()
	require.NoError(t, b.UnmarshalBinary(dataBuf))
	require.Equal(t, 2, int(b.Count()))
	require.True(t, b.Contains(2))
	require.True(t, b.Contains(3))
}

func TestWriterFullDocIDSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ds := &fullDocIDSet{numTotalDocs: 4}
	writer := digest.NewMockFdWithDigestWriter(ctrl)
	gomock.InOrder(
		writer.EXPECT().Write([]byte{0}).Return(1, nil),
		writer.EXPECT().Write([]byte{8}).Return(1, nil),
	)

	require.NoError(t, ds.WriteTo(writer, bytes.NewBuffer(nil)))
}
