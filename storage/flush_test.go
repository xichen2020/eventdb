package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseWriteBatch(t *testing.T) {
	db, err := newTestDatabase()
	require.NoError(t, err)
	defer db.Close()

	ns, docs, err := createTestDocuments()
	require.NoError(t, err)
	require.NoError(t, db.WriteBatch(context.Background(), ns, docs))
}
