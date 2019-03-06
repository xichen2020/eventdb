package client

import (
	"context"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/query"
)

// HealthResult contains the result for health checks.
type HealthResult struct {
	IsHealthy bool
	StatusMsg string
}

// Client is the database client.
type Client interface {
	// Health performs a health check against the database.
	Health() (*HealthResult, error)

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
		q query.UnparsedTimeBucketQuery,
	) (*query.TimeBucketQueryResults, error)

	// Close closes the client.
	Close() error
}
