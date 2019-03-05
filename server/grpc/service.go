package grpc

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/storage"
	"github.com/xichen2020/eventdb/x/proto/convert"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/uber-go/tally"
)

const (
	batchSizeBucketVersion = 1
	bucketSize             = 500
	numBuckets             = 40
)

type serviceMetrics struct {
	write           instrument.MethodMetrics
	queryRaw        instrument.MethodMetrics
	queryGrouped    instrument.MethodMetrics
	queryTimeBucket instrument.MethodMetrics
	batchSizeHist   tally.Histogram
}

func newServiceMetrics(
	scope tally.Scope,
	samplingRate float64,
) serviceMetrics {
	batchSizeBuckets := tally.MustMakeLinearValueBuckets(0, bucketSize, numBuckets)
	return serviceMetrics{
		write:           instrument.NewMethodMetrics(scope, "write", samplingRate),
		queryRaw:        instrument.NewMethodMetrics(scope, "queryRaw", samplingRate),
		queryGrouped:    instrument.NewMethodMetrics(scope, "queryGrouped", samplingRate),
		queryTimeBucket: instrument.NewMethodMetrics(scope, "queryTimeBucket", samplingRate),
		batchSizeHist: scope.Tagged(map[string]string{
			"bucket-version": strconv.Itoa(batchSizeBucketVersion),
		}).Histogram("batch-size", batchSizeBuckets),
	}
}

var (
	errNilWriteRequest    = errors.New("nil write request")
	errNilRawQuery        = errors.New("nil raw query")
	errNilGroupedQuery    = errors.New("nil grouped query")
	errNilTimeBucketQuery = errors.New("nil time bucket query")
)

// service serves read and write requests.
type service struct {
	db                storage.Database
	parseOpts         query.ParseOptions
	readTimeout       time.Duration
	writeTimeout      time.Duration
	documentArrayPool *document.BucketizedDocumentArrayPool
	fieldArrayPool    *field.BucketizedFieldArrayPool

	nowFn   clock.NowFn
	metrics serviceMetrics
}

// NewService creates a new service.
func NewService(db storage.Database, opts *ServiceOptions) servicepb.EventdbServer {
	if opts == nil {
		opts = NewServiceOptions()
	}
	var (
		dbOpts         = db.Options()
		instrumentOpts = opts.InstrumentOptions()
		scope          = instrumentOpts.MetricsScope()
		samplingRate   = instrumentOpts.MetricsSamplingRate()
	)
	parseOpts := query.ParseOptions{
		FieldPathSeparator:    dbOpts.FieldPathSeparator(),
		FieldHashFn:           dbOpts.FieldHashFn(),
		TimestampFieldPath:    dbOpts.TimestampFieldPath(),
		RawDocSourceFieldPath: dbOpts.RawDocSourceFieldPath(),
	}
	return &service{
		db:                db,
		parseOpts:         parseOpts,
		readTimeout:       opts.ReadTimeout(),
		writeTimeout:      opts.WriteTimeout(),
		documentArrayPool: opts.DocumentArrayPool(),
		fieldArrayPool:    opts.FieldArrayPool(),
		nowFn:             opts.ClockOptions().NowFn(),
		metrics:           newServiceMetrics(scope, samplingRate),
	}
}

func (s *service) Write(
	ctx context.Context,
	req *servicepb.WriteRequest,
) (*servicepb.WriteResults, error) {
	if req == nil {
		return nil, errNilWriteRequest
	}

	callStart := s.nowFn()

	ctx, cancelFn := context.WithTimeout(ctx, s.writeTimeout)
	defer cancelFn()

	s.metrics.batchSizeHist.RecordValue(float64(len(req.Docs)))

	docs, err := convert.ToDocuments(req.Docs, s.documentArrayPool, s.fieldArrayPool)
	if err != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	if err = s.db.WriteBatch(ctx, req.Namespace, docs); err != nil {
		document.ReturnArrayToPool(docs, s.documentArrayPool)
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	document.ReturnArrayToPool(docs, s.documentArrayPool)
	s.metrics.write.ReportSuccess(s.nowFn().Sub(callStart))
	return &servicepb.WriteResults{}, nil
}

func (s *service) QueryRaw(
	ctx context.Context,
	q *servicepb.RawQuery,
) (*servicepb.RawQueryResults, error) {
	if q == nil {
		return nil, errNilRawQuery
	}

	callStart := s.nowFn()

	ctx, cancelFn := context.WithTimeout(ctx, s.readTimeout)
	defer cancelFn()

	unparsed, err := query.ToUnparsedRawQuery(q)
	if err != nil {
		s.metrics.queryRaw.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	rawQuery, err := unparsed.Parse(s.parseOpts)
	if err != nil {
		s.metrics.queryRaw.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := s.db.QueryRaw(ctx, rawQuery)
	if err != nil {
		s.metrics.queryRaw.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	pbRes := res.ToProto()
	s.metrics.queryRaw.ReportSuccess(s.nowFn().Sub(callStart))
	return pbRes, nil
}

func (s *service) QueryGrouped(
	ctx context.Context,
	q *servicepb.GroupedQuery,
) (*servicepb.GroupedQueryResults, error) {
	if q == nil {
		return nil, errNilGroupedQuery
	}
	callStart := s.nowFn()

	ctx, cancelFn := context.WithTimeout(ctx, s.readTimeout)
	defer cancelFn()

	unparsed, err := query.ToUnparsedGroupedQuery(q)
	if err != nil {
		s.metrics.queryGrouped.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	groupedQuery, err := unparsed.Parse(s.parseOpts)
	if err != nil {
		s.metrics.queryGrouped.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := s.db.QueryGrouped(ctx, groupedQuery)
	if err != nil {
		s.metrics.queryGrouped.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	pbRes, err := res.ToProto()
	if err != nil {
		s.metrics.queryGrouped.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	s.metrics.queryGrouped.ReportSuccess(s.nowFn().Sub(callStart))
	return pbRes, nil
}

func (s *service) QueryTimeBucket(
	ctx context.Context,
	q *servicepb.TimeBucketQuery,
) (*servicepb.TimeBucketQueryResults, error) {
	if q == nil {
		return nil, errNilTimeBucketQuery
	}
	callStart := s.nowFn()

	ctx, cancelFn := context.WithTimeout(ctx, s.readTimeout)
	defer cancelFn()

	unparsed, err := query.ToUnparsedTimeBucketQuery(q)
	if err != nil {
		s.metrics.queryTimeBucket.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	timeBucketQuery, err := unparsed.Parse(s.parseOpts)
	if err != nil {
		s.metrics.queryTimeBucket.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := s.db.QueryTimeBucket(ctx, timeBucketQuery)
	if err != nil {
		s.metrics.queryTimeBucket.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	pbRes := res.ToProto()
	s.metrics.queryTimeBucket.ReportSuccess(s.nowFn().Sub(callStart))
	return pbRes, nil
}
