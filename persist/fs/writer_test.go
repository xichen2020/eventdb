package fs

import (
	"encoding/binary"
	"testing"

	"github.com/xichen2020/eventdb/digest"
	"github.com/xichen2020/eventdb/persist/schema"

	"github.com/golang/mock/gomock"
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

func TestWritePartialDocIDSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		sizeBuf []byte
		dataBuf []byte
	)
	w := &writer{}
	w.numDocuments = 4
	docIDSet := docIDSetUnion{
		docIDSetType: schema.PartialDocIDSet,
		docIDs:       roaring.NewBitmap(2, 3),
	}
	writer := digest.NewMockFdWithDigestWriter(ctrl)
	gomock.InOrder(
		writer.EXPECT().Write([]byte{0}).Return(1, nil),
		writer.EXPECT().Write(gomock.Any()).DoAndReturn(func(data []byte) (int, error) {
			sizeBuf = make([]byte, len(data))
			copy(sizeBuf, data)
			return 1, nil
		}),
		writer.EXPECT().Write(gomock.Any()).DoAndReturn(func(data []byte) (int, error) {
			dataBuf = make([]byte, len(data))
			copy(dataBuf, data)
			return len(data), nil
		}),
	)

	require.NoError(t, w.writeDocIDSet(writer, docIDSet))
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

	w := &writer{}
	w.numDocuments = 4
	docIDSet := docIDSetUnion{
		docIDSetType: schema.FullDocIDSet,
		numDocs:      w.numDocuments,
	}
	writer := digest.NewMockFdWithDigestWriter(ctrl)
	gomock.InOrder(
		writer.EXPECT().Write([]byte{1}).Return(1, nil),
		writer.EXPECT().Write([]byte{8}).Return(1, nil),
	)

	require.NoError(t, w.writeDocIDSet(writer, docIDSet))
}
