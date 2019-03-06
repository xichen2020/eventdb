package grpc

import (
	"context"
	"strconv"
	"time"

	"github.com/xichen2020/eventdb/client"
	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/query"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip" // Register gzip compressor
	"google.golang.org/grpc/keepalive"
)

const (
	batchSizeBucketVersion = 1
	bucketSize             = 500
	numBuckets             = 40
)

type clientMetrics struct {
	write           instrument.MethodMetrics
	queryRaw        instrument.MethodMetrics
	queryGrouped    instrument.MethodMetrics
	queryTimeBucket instrument.MethodMetrics
	batchSizeHist   tally.Histogram
}

func newClientMetrics(
	scope tally.Scope,
	samplingRate float64,
) clientMetrics {
	batchSizeBuckets := tally.MustMakeLinearValueBuckets(0, bucketSize, numBuckets)
	return clientMetrics{
		write:           instrument.NewMethodMetrics(scope, "write", samplingRate),
		queryRaw:        instrument.NewMethodMetrics(scope, "queryRaw", samplingRate),
		queryGrouped:    instrument.NewMethodMetrics(scope, "queryGrouped", samplingRate),
		queryTimeBucket: instrument.NewMethodMetrics(scope, "queryTimeBucket", samplingRate),
		batchSizeHist: scope.Tagged(map[string]string{
			"bucket-version": strconv.Itoa(batchSizeBucketVersion),
		}).Histogram("batch-size", batchSizeBuckets),
	}
}

// Client is a GRPC client.
type Client struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	conn         *grpc.ClientConn
	client       servicepb.EventdbClient

	metrics clientMetrics
	nowFn   clock.NowFn
}

// NewClient creates a new client.
func NewClient(
	address string,
	opts *Options,
) (*Client, error) {
	if opts == nil {
		opts = NewOptions()
	}

	dialOpts := []grpc.DialOption{
		grpc.WithWriteBufferSize(opts.WriteBufferSize()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: opts.KeepAlivePeriod(),
		}),
	}
	if opts.UseInsecure() {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	callOpts := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(opts.MaxRecvMsgSize()),
	}
	if opts.UseCompression() {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(callOpts...))

	var (
		ctx      = context.Background()
		cancelFn context.CancelFunc
	)
	if opts.DialTimeout() != 0 {
		dialOpts = append(dialOpts, grpc.WithBlock())
		ctx, cancelFn = context.WithTimeout(ctx, opts.DialTimeout())
		defer cancelFn()
	}
	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		return nil, err
	}
	client := servicepb.NewEventdbClient(conn)
	instrumentOpts := opts.InstrumentOptions()
	return &Client{
		readTimeout:  opts.ReadTimeout(),
		writeTimeout: opts.WriteTimeout(),
		conn:         conn,
		client:       client,
		metrics:      newClientMetrics(instrumentOpts.MetricsScope(), instrumentOpts.MetricsSamplingRate()),
		nowFn:        opts.ClockOptions().NowFn(),
	}, nil
}

// Health performs a health check against the database.
func (c *Client) Health() (*client.HealthResult, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), c.readTimeout)
	defer cancelFn()

	req := &servicepb.HealthRequest{}
	pbRes, err := c.client.Health(ctx, req)
	if err != nil {
		return nil, err
	}
	return &client.HealthResult{
		IsHealthy: pbRes.IsHealthy,
		StatusMsg: pbRes.StatusMsg,
	}, nil
}

// Write writes a batch of documents.
func (c *Client) Write(
	ctx context.Context,
	namespace []byte,
	documents []document.Document,
) error {
	callStart := c.nowFn()

	pbDocs, err := document.Documents(documents).ToProto()
	if err != nil {
		c.metrics.write.ReportError(c.nowFn().Sub(callStart))
		return err
	}

	ctx, cancelFn := context.WithTimeout(ctx, c.writeTimeout)
	defer cancelFn()

	req := &servicepb.WriteRequest{
		Namespace: namespace,
		Docs:      pbDocs,
	}
	_, err = c.client.Write(ctx, req)
	c.metrics.write.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

// QueryRaw performs a raw query.
func (c *Client) QueryRaw(
	ctx context.Context,
	q query.UnparsedRawQuery,
) (*query.RawQueryResults, error) {
	callStart := c.nowFn()

	pbRawQuery, err := q.ToProto()
	if err != nil {
		c.metrics.queryRaw.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	ctx, cancelFn := context.WithTimeout(ctx, c.readTimeout)
	defer cancelFn()

	pbRes, err := c.client.QueryRaw(ctx, pbRawQuery)
	if err != nil {
		c.metrics.queryRaw.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := query.NewRawQueryResultsFromProto(pbRes)
	if err != nil {
		c.metrics.queryRaw.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	c.metrics.queryRaw.ReportSuccess(c.nowFn().Sub(callStart))
	return res, nil
}

// QueryGrouped performs a grouped query.
func (c *Client) QueryGrouped(
	ctx context.Context,
	q query.UnparsedGroupedQuery,
) (*query.GroupedQueryResults, error) {
	callStart := c.nowFn()

	pbGroupedQuery, err := q.ToProto()
	if err != nil {
		c.metrics.queryGrouped.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	ctx, cancelFn := context.WithTimeout(ctx, c.readTimeout)
	defer cancelFn()

	pbRes, err := c.client.QueryGrouped(ctx, pbGroupedQuery)
	if err != nil {
		c.metrics.queryGrouped.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := query.NewGroupedQueryResultsFromProto(pbRes)
	if err != nil {
		c.metrics.queryGrouped.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	c.metrics.queryGrouped.ReportSuccess(c.nowFn().Sub(callStart))
	return res, nil
}

// QueryTimeBucket performs a time bucket query.
func (c *Client) QueryTimeBucket(
	ctx context.Context,
	q query.UnparsedTimeBucketQuery,
) (*query.TimeBucketQueryResults, error) {
	callStart := c.nowFn()

	pbTimeBucketQuery, err := q.ToProto()
	if err != nil {
		c.metrics.queryTimeBucket.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	ctx, cancelFn := context.WithTimeout(ctx, c.readTimeout)
	defer cancelFn()

	pbRes, err := c.client.QueryTimeBucket(ctx, pbTimeBucketQuery)
	if err != nil {
		c.metrics.queryTimeBucket.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := query.NewTimeBucketQueryResultsFromProto(pbRes)
	if err != nil {
		c.metrics.queryTimeBucket.ReportError(c.nowFn().Sub(callStart))
		return nil, err
	}

	c.metrics.queryTimeBucket.ReportSuccess(c.nowFn().Sub(callStart))
	return res, nil
}

// Close closes the client.
func (c *Client) Close() error {
	var err error
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	return err
}
