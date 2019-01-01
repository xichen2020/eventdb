package document

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullDocIDSetIterator(t *testing.T) {
	var docIDs []int32
	it := NewFullDocIDSetIterator(6)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
	}
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5}, docIDs)
}
