package index

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyDocIDSetIterator(t *testing.T) {
	var docIDs []int32
	it := NewEmptyDocIDSetIterator()
	defer it.Close()

	for it.Next() {
		docIDs = append(docIDs, it.DocID())
	}
	require.NoError(t, it.Err())
	require.Equal(t, 0, len(docIDs))
}
