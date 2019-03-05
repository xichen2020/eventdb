package client

import (
	"context"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/query"
)

// Client is the database client.
type Client interface {
	// Write writes a batch of documents.
	Write(
		ctx context.Context,
		namespace []byte,
		documents []document.Document,
	) error

	// QueryRaw performs a raw query.
	QueryRaw(
		ctx context.Context,
		q query.UnparsedRawQuery,
	) (*query.RawQueryResults, error)

	// QueryGrouped performs a grouped query.
	QueryGrouped(
		ctx context.Context,
		q query.UnparsedGroupedQuery,
	) (*query.GroupedQueryResults, error)

	// QueryTimeBucket performs a time bucket query.
	QueryTimeBucket(
		ctx context.Context,
		q query.UnparsedGroupedQuery,
	) (*query.TimeBucketQueryResults, error)

	// Close closes the client.
	Close() error
}
