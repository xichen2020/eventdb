package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlush(t *testing.T) {
	tdb, err := newTestDatabase()
	require.Nil(t, err)
	defer tdb.Close()

	ns, docs, err := createTestDocuments()
	require.Nil(t, err)

	err = tdb.WriteBatch(ns, docs)
	require.Nil(t, err)

	// Get a reference to the underlying fs manager and force a flush.
	db := tdb.Database.(*db)
	mediator := db.mediator.(*mediator)
	fsMgr := mediator.databaseFileSystemManager.(*fileSystemManager)
	err = fsMgr.databaseFlushManager.Flush()
	require.Nil(t, err)
}
