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

const (
	defaultReadTimeout     = time.Minute
	defaultWriteTimeout    = time.Minute
	batchSizeBucketVersion = 1
	bucketSize             = 500
	numBuckets             = 40
)

// ServiceOptions provide a set of service options.
type ServiceOptions struct {
	clockOpts         clock.Options
	instrumentOpts    instrument.Options
	readTimeout       time.Duration
	writeTimeout      time.Duration
	documentArrayPool *document.BucketizedDocumentArrayPool
	fieldArrayPool    *field.BucketizedFieldArrayPool
}

// NewServiceOptions create a new set of service options.
func NewServiceOptions() *ServiceOptions {
	o := &ServiceOptions{
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		readTimeout:    defaultReadTimeout,
		writeTimeout:   defaultWriteTimeout,
	}
	o.initPools()
	return o
}

// SetClockOptions sets the clock options.
func (o *ServiceOptions) SetClockOptions(v clock.Options) *ServiceOptions {
	opts := *o
	opts.clockOpts = v
	return &opts
}

// ClockOptions returns the clock options.
func (o *ServiceOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

// SetInstrumentOptions sets the instrument options.
func (o *ServiceOptions) SetInstrumentOptions(v instrument.Options) *ServiceOptions {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *ServiceOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetReadTimeout sets the read timeout.
func (o *ServiceOptions) SetReadTimeout(v time.Duration) *ServiceOptions {
	opts := *o
	opts.readTimeout = v
	return &opts
}

// ReadTimeout returns the read timeout.
func (o *ServiceOptions) ReadTimeout() time.Duration {
	return o.readTimeout
}

// SetWriteTimeout sets the write timeout.
func (o *ServiceOptions) SetWriteTimeout(v time.Duration) *ServiceOptions {
	opts := *o
	opts.writeTimeout = v
	return &opts
}

// WriteTimeout returns the write timeout.
func (o *ServiceOptions) WriteTimeout() time.Duration {
	return o.writeTimeout
}

// SetDocumentArrayPool sets the document array pool.
func (o *ServiceOptions) SetDocumentArrayPool(v *document.BucketizedDocumentArrayPool) *ServiceOptions {
	opts := *o
	opts.documentArrayPool = v
	return &opts
}

// DocumentArrayPool returns the document array pool.
func (o *ServiceOptions) DocumentArrayPool() *document.BucketizedDocumentArrayPool {
	return o.documentArrayPool
}

// SetFieldArrayPool sets the field array pool.
func (o *ServiceOptions) SetFieldArrayPool(v *field.BucketizedFieldArrayPool) *ServiceOptions {
	opts := *o
	opts.fieldArrayPool = v
	return &opts
}

// FieldArrayPool returns the field array pool.
func (o *ServiceOptions) FieldArrayPool() *field.BucketizedFieldArrayPool {
	return o.fieldArrayPool
}

func (o *ServiceOptions) initPools() {
	documentArrayPool := document.NewBucketizedDocumentArrayPool(nil, nil)
	documentArrayPool.Init(func(capacity int) []document.Document {
		return make([]document.Document, 0, capacity)
	})
	o.documentArrayPool = documentArrayPool

	fieldArrayPool := field.NewBucketizedFieldArrayPool(nil, nil)
	fieldArrayPool.Init(func(capacity int) []field.Field {
		return make([]field.Field, 0, capacity)
	})
	o.fieldArrayPool = fieldArrayPool
}

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
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	samplingRate := instrumentOpts.MetricsSamplingRate()
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
		document.ReturnArrayToPool(docs, s.documentArrayPool)
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
