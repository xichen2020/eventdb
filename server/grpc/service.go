package grpc

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/server/grpc/convert"
	"github.com/xichen2020/eventdb/storage"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/uber-go/tally"
)

type serviceMetrics struct {
	write         instrument.MethodMetrics
	batchSizeHist tally.Histogram
}

func newServiceMetrics(
	scope tally.Scope,
	samplingRate float64,
) serviceMetrics {
	batchSizeBuckets := tally.MustMakeLinearValueBuckets(0, bucketSize, numBuckets)
	return serviceMetrics{
		write: instrument.NewMethodMetrics(scope, "write", samplingRate),
		batchSizeHist: scope.Tagged(map[string]string{
			"bucket-version": strconv.Itoa(batchSizeBucketVersion),
		}).Histogram("batch-size", batchSizeBuckets),
	}
}

var (
	errWriteNilDocuments = errors.New("attempt to write nil documents")
)

// service serves read and write requests.
type service struct {
	db                storage.Database
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
		instrumentOpts = opts.InstrumentOptions()
		scope          = instrumentOpts.MetricsScope()
		samplingRate   = instrumentOpts.MetricsSamplingRate()
	)
	return &service{
		db:                db,
		readTimeout:       opts.ReadTimeout(),
		writeTimeout:      opts.WriteTimeout(),
		documentArrayPool: opts.DocumentArrayPool(),
		fieldArrayPool:    opts.FieldArrayPool(),
		nowFn:             opts.ClockOptions().NowFn(),
		metrics:           newServiceMetrics(scope, samplingRate),
	}
}

// Write writes a list of documents to the database.
func (s *service) Write(
	ctx context.Context,
	pbDocs *servicepb.Documents,
) (*servicepb.WriteResponse, error) {
	if pbDocs == nil {
		return nil, errWriteNilDocuments
	}

	callStart := s.nowFn()

	ctx, cancelFn := context.WithTimeout(ctx, s.writeTimeout)
	defer cancelFn()

	s.metrics.batchSizeHist.RecordValue(float64(len(pbDocs.Docs)))

	docs, err := convert.ToDocuments(pbDocs.Docs, s.documentArrayPool, s.fieldArrayPool)
	if err != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	if err = s.db.WriteBatch(ctx, pbDocs.Namespace, docs); err != nil {
		document.ReturnArrayToPool(docs, s.documentArrayPool)
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return nil, err
	}

	document.ReturnArrayToPool(docs, s.documentArrayPool)
	s.metrics.write.ReportSuccess(s.nowFn().Sub(callStart))
	return &servicepb.WriteResponse{}, nil
}
