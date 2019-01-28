package integration

import (
	"github.com/xichen2020/eventdb/services/eventdb/config"
)

// configuration wraps config.Configuration with extra fields necessary for integration testing.
type configuration struct {
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
http:
  listenAddress: 0.0.0.0:5678
  readTimeout: 1m
  writeTimeout: 1m
  handler:
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
  filePathPrefix: /var/lib/eventdb
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
  contextPool:
    size: 128
    lowWatermark: 0.7
    highWatermark: 1.0
    maxFinalizerCapacity: 65536
  boolArrayPool: # total < 1GB
    buckets:
      - count: 5000
        capacity: 4096
      - count: 5000
        capacity: 8192
      - count: 5000
        capacity: 16384
      - count: 5000
        capacity: 32768
      - count: 5000
        capacity: 65536
    watermark:
      low: 0.7
      high: 1.0
  intArrayPool: # total < 1GB
    buckets:
      - count: 1000
        capacity: 4096
      - count: 1000
        capacity: 8192
      - count: 1000
        capacity: 16384
      - count: 1000
        capacity: 32768
      - count: 1000
        capacity: 65536
    watermark:
      low: 0.7
      high: 1.0
  int64ArrayPool: # For timestamps, 8 shards * 2 segments = 16, ~250MB
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
  doubleArrayPool: # total < 1GB
    buckets:
      - count: 1000
        capacity: 4096
      - count: 1000
        capacity: 8192
      - count: 1000
        capacity: 16384
      - count: 1000
        capacity: 32768
      - count: 1000
        capacity: 65536
    watermark:
      low: 0.7
      high: 1.0
  stringArrayPool: # 1K string fields * 8 shards * 2 segments = 16K, ~15G
    buckets:
      - count: 1600
        capacity: 8192
      - count: 1600
        capacity: 16384
      - count: 1600
        capacity: 32768
      - count: 1600
        capacity: 65536
      - count: 1600
        capacity: 131072
      - count: 1600
        capacity: 262144
      - count: 1600
        capacity: 524288
    watermark:
      low: 0.7
      high: 1.0
`

	testData1 = `
{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`
)
