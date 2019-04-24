package config

import (
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/server/grpc"

	"github.com/m3db/m3/src/x/instrument"
)

// GRPCServerConfiguration contains gRPC server configuration.
type GRPCServerConfiguration struct {
	// HTTP server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// Read buffer size.
	ReadBufferSize *int `yaml:"readBufferSize"`

	// Max receive message size.
	MaxRecvMsgSize *int `yaml:"maxRecvMsgSize"`

	// KeepAlive period.
	KeepAlivePeriod *time.Duration `yaml:"keepAlivePeriod"`

	// Service configuration.
	Service grpcServiceConfiguration `yaml:"service"`
}

// NewServerOptions create a new set of gRPC server options.
func (c *GRPCServerConfiguration) NewServerOptions(
	instrumentOpts instrument.Options,
) *grpc.Options {
	opts := grpc.NewOptions().SetInstrumentOptions(instrumentOpts)
	if c.ReadBufferSize != nil {
		opts = opts.SetReadBufferSize(*c.ReadBufferSize)
	}
	if c.MaxRecvMsgSize != nil {
		opts = opts.SetMaxRecvMsgSize(*c.MaxRecvMsgSize)
	}
	if c.KeepAlivePeriod != nil {
		opts = opts.SetKeepAlivePeriod(*c.KeepAlivePeriod)
	}
	return opts
}

type grpcServiceConfiguration struct {
	// Read timeout.
	ReadTimeout *time.Duration `yaml:"readTimeout"`

	// Write timeout.
	WriteTimeout *time.Duration `yaml:"writeTimeout"`

	// Document array pool.
	DocumentArrayPool *document.BucketizedDocumentArrayPoolConfiguration `yaml:"documentArrayPool"`

	// Field array pool.
	FieldArrayPool *field.BucketizedFieldArrayPoolConfiguration `yaml:"fieldArrayPool"`
}

func (c *grpcServiceConfiguration) NewOptions(
	instrumentOpts instrument.Options,
) *grpc.ServiceOptions {
	opts := grpc.NewServiceOptions()
	if c.ReadTimeout != nil {
		opts = opts.SetReadTimeout(*c.ReadTimeout)
	}
	if c.WriteTimeout != nil {
		opts = opts.SetWriteTimeout(*c.WriteTimeout)
	}
	scope := instrumentOpts.MetricsScope()
	if c.DocumentArrayPool != nil {
		buckets := c.DocumentArrayPool.NewBuckets()
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("document-array-pool"))
		poolOpts := c.DocumentArrayPool.NewPoolOptions(iOpts)
		documentArrayPool := document.NewBucketizedDocumentArrayPool(buckets, poolOpts)
		documentArrayPool.Init(func(capacity int) []document.Document {
			return make([]document.Document, 0, capacity)
		})
		opts = opts.SetDocumentArrayPool(documentArrayPool)
	}
	if c.FieldArrayPool != nil {
		buckets := c.FieldArrayPool.NewBuckets()
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("field-array-pool"))
		poolOpts := c.FieldArrayPool.NewPoolOptions(iOpts)
		fieldArrayPool := field.NewBucketizedFieldArrayPool(buckets, poolOpts)
		fieldArrayPool.Init(func(capacity int) []field.Field {
			return make([]field.Field, 0, capacity)
		})
		opts = opts.SetFieldArrayPool(fieldArrayPool)
	}
	return opts
}
