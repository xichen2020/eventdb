package integration

import (
	"github.com/xichen2020/eventdb/services/eventdb/config"
)

// configuration wraps config.Configuration with extra fields necessary for integration testing.
type configuration struct {
	// GRPC server configuration.
	GRPC config.GRPCServerConfiguration `yaml:"grpc"`

	// HTTP server configuration.
	HTTP config.HTTPServerConfiguration `yaml:"http"`

	// Database configuration.
	Database config.DatabaseConfiguration `yaml:"database"`
}

// NB(wjang): When running the integration tests via `test-ci-integration`, the test binaries are
// prebuilt and run. The issue is that we used to rely on relative paths to load the  config and
// testdata files for our tests; these relative paths break when we run the binary tests. To get
// around this, we hard coded the configs here.
const (
	// NB(wjang): YAML does NOT allow tabs for indentation between levels, remember to use spaces.
	testConfig1 = `
grpc:
  listenAddress: 0.0.0.0:5678
  readBufferSize: 1048576 # 1MB
  maxRecvMsgSize: 67108864 # 64MB
  keepAlivePeriod: 10m
  service:
    readTimeout: 1m
    writeTimeout: 1m
    documentArrayPool:
      buckets:
        - count: 16
          capacity: 1024
        - count: 16
          capacity: 2048
        - count: 16
          capacity: 4096
        - count: 16
          capacity: 8192
        - count: 16
          capacity: 16384
        - count: 16
          capacity: 32768
      watermark:
        low: 0.7
        high: 1.0
    fieldArrayPool:
      buckets:
        - count: 16
          capacity: 64
        - count: 16
          capacity: 128
        - count: 16
          capacity: 256
        - count: 16
          capacity: 512
        - count: 16
          capacity: 1024
      watermark:
        low: 0.7
        high: 1.0

http:
  listenAddress: 0.0.0.0:5679
  service:
    readTimeout: 1m
    writeTimeout: 1m
    parserPool:
      size: 50000
      watermark:
        low: 0.001
        high: 0.002
    parser:
      maxDepth: 3
      excludeKeySuffix: _noindex

database:
  namespaces:
    - id: testNamespace
      retention: 87600h # 10 years in hours, DB has a default data retention of 24 hours that will break fixtures if we run them in the future (fixtures have hard coded timestamps).
  numShards: 8
  filePathPrefix: /tmp/eventdb
  fieldPathSeparator: .
  namespaceFieldName: service
  timestampFieldName: "@timestamp"
  tickMinInterval: 1m
  maxNumDocsPerSegment: 1048576
  segmentUnloadAfterUnreadFor: 5m
  persist:
    writeBufferSize: 65536
    readBufferSize: 65536
    timestampPrecision: 1ms
    mmapEnableHugePages: true
    mmapHugePagesThreshold: 16384 # 2 ^ 14
  boolArrayPool:
    buckets:
      - count: 50
        capacity: 4096
      - count: 50
        capacity: 8192
      - count: 50
        capacity: 16384
      - count: 50
        capacity: 32768
      - count: 50
        capacity: 65536
    watermark:
      low: 0.7
      high: 1.0
  intArrayPool:
    buckets:
      - count: 10
        capacity: 4096
      - count: 10
        capacity: 8192
      - count: 10
        capacity: 16384
      - count: 10
        capacity: 32768
      - count: 10
        capacity: 65536
    watermark:
      low: 0.7
      high: 1.0
  int64ArrayPool:
    buckets:
      - count: 32
        capacity: 65536
      - count: 32
        capacity: 131072
      - count: 32
        capacity: 262144
      - count: 32
        capacity: 524288
    watermark:
      low: 0.001
      high: 0.002
  doubleArrayPool:
    buckets:
      - count: 10
        capacity: 4096
      - count: 10
        capacity: 8192
      - count: 10
        capacity: 16384
      - count: 10
        capacity: 32768
      - count: 10
        capacity: 65536
    watermark:
      low: 0.7
      high: 1.0
  stringArrayPool:
    buckets:
      - count: 16
        capacity: 8192
      - count: 16
        capacity: 16384
      - count: 16
        capacity: 32768
      - count: 16
        capacity: 65536
      - count: 16
        capacity: 131072
      - count: 16
        capacity: 262144
      - count: 16
        capacity: 524288
    watermark:
      low: 0.7
      high: 1.0
`
)
