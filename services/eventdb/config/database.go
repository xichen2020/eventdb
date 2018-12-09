package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/persist/fs"
	"github.com/xichen2020/eventdb/storage"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/uber-go/tally"
)

var (
	errNoPersistManagerConfig = errors.New("no persist manager configuration provided")
)

// DatabaseConfiguration provides database configuration.
type DatabaseConfiguration struct {
	Namespaces                   namespaces                                    `yaml:"namespaces"`
	NumShards                    int                                           `yaml:"numShards"`
	FieldPathSeparator           *separator                                    `yaml:"fieldPathSeparator"`
	NamespaceFieldName           *string                                       `yaml:"namespaceFieldName"`
	TimestampFieldName           *string                                       `yaml:"timestampFieldName"`
	MinRunInterval               *time.Duration                                `yaml:"minRunInterval"`
	MaxNumCachedSegmentsPerShard *int                                          `yaml:"maxNumCachedSegmentsPerShard"`
	MaxNumDocsPerSegment         *int32                                        `yaml:"maxNumDocsPerSegment"`
	PersistManager               *persistManagerConfiguration                  `yaml:"persist"`
	BoolArrayPool                *pool.BucketizedBoolArrayPoolConfiguration    `yaml:"boolArrayPool"`
	IntArrayPool                 *pool.BucketizedIntArrayPoolConfiguration     `yaml:"intArrayPool"`
	Int64ArrayPool               *pool.BucketizedInt64ArrayPoolConfiguration   `yaml:"int64ArrayPool"`
	DoubleArrayPool              *pool.BucketizedFloat64ArrayPoolConfiguration `yaml:"doubleArrayPool"`
	StringArrayPool              *pool.BucketizedStringArrayPoolConfiguration  `yaml:"stringArrayPool"`
}

// NewOptions create a new set of database options from configuration.
func (c *DatabaseConfiguration) NewOptions(scope tally.Scope) (*storage.Options, error) {
	if c.PersistManager == nil {
		return nil, errNoPersistManagerConfig
	}
	opts := storage.NewOptions()
	if c.FieldPathSeparator != nil {
		opts = opts.SetFieldPathSeparator(byte(*c.FieldPathSeparator))
	}
	if c.NamespaceFieldName != nil {
		opts = opts.SetNamespaceFieldName(*c.NamespaceFieldName)
	}
	if c.TimestampFieldName != nil {
		opts = opts.SetTimestampFieldName(*c.TimestampFieldName)
	}
	if c.MinRunInterval != nil {
		opts = opts.SetMinRunInterval(*c.MinRunInterval)
	}
	if c.MaxNumCachedSegmentsPerShard != nil {
		opts = opts.SetMaxNumCachedSegmentsPerShard(*c.MaxNumCachedSegmentsPerShard)
	}
	if c.MaxNumDocsPerSegment != nil {
		opts = opts.SetMaxNumDocsPerSegment(*c.MaxNumDocsPerSegment)
	}
	persistManager := c.PersistManager.NewPersistManager(opts.FieldPathSeparator(), opts.TimestampFieldName())
	opts = opts.SetPersistManager(persistManager)

	// Initialize various pools.
	if c.BoolArrayPool != nil {
		buckets := c.BoolArrayPool.NewBuckets()
		poolOpts := c.BoolArrayPool.NewPoolOptions(scope.SubScope("bool-array-pool"))
		boolArrayPool := pool.NewBucketizedBoolArrayPool(buckets, poolOpts)
		boolArrayPool.Init(func(capacity int) []bool { return make([]bool, 0, capacity) })
		opts = opts.SetBoolArrayPool(boolArrayPool)
	}
	if c.IntArrayPool != nil {
		buckets := c.IntArrayPool.NewBuckets()
		poolOpts := c.IntArrayPool.NewPoolOptions(scope.SubScope("int-array-pool"))
		intArrayPool := pool.NewBucketizedIntArrayPool(buckets, poolOpts)
		intArrayPool.Init(func(capacity int) []int { return make([]int, 0, capacity) })
		opts = opts.SetIntArrayPool(intArrayPool)
	}
	if c.Int64ArrayPool != nil {
		buckets := c.Int64ArrayPool.NewBuckets()
		poolOpts := c.Int64ArrayPool.NewPoolOptions(scope.SubScope("int64-array-pool"))
		int64ArrayPool := pool.NewBucketizedInt64ArrayPool(buckets, poolOpts)
		int64ArrayPool.Init(func(capacity int) []int64 { return make([]int64, 0, capacity) })
		opts = opts.SetInt64ArrayPool(int64ArrayPool)
	}
	if c.DoubleArrayPool != nil {
		buckets := c.DoubleArrayPool.NewBuckets()
		poolOpts := c.DoubleArrayPool.NewPoolOptions(scope.SubScope("double-array-pool"))
		doubleArrayPool := pool.NewBucketizedFloat64ArrayPool(buckets, poolOpts)
		doubleArrayPool.Init(func(capacity int) []float64 { return make([]float64, 0, capacity) })
		opts = opts.SetDoubleArrayPool(doubleArrayPool)
	}
	if c.StringArrayPool != nil {
		buckets := c.StringArrayPool.NewBuckets()
		poolOpts := c.StringArrayPool.NewPoolOptions(scope.SubScope("string-array-pool"))
		stringArrayPool := pool.NewBucketizedStringArrayPool(buckets, poolOpts)
		stringArrayPool.Init(func(capacity int) []string { return make([]string, 0, capacity) })
		opts = opts.SetStringArrayPool(stringArrayPool)
	}
	return opts, nil
}

// namespaces is a custom type for enforcing unmarshaling rules.
type namespaces [][]byte

// UnmarshalYAML implements the Unmarshaler interface for the `namespaces` type.
func (n *namespaces) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var namespaces []string
	if err := unmarshal(&namespaces); err != nil {
		return err
	}

	nss := make([][]byte, 0, len(namespaces))
	for _, ns := range namespaces {
		nss = append(nss, []byte(ns))
	}

	*n = nss
	return nil
}

// separator is a custom type for unmarshaling a single character.
type separator byte

// UnmarshalYAML implements the Unmarshaler interface for the `Separator` type.
func (s *separator) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var separatorStr string
	if err := unmarshal(&separatorStr); err != nil {
		return err
	}
	if len(separatorStr) != 1 {
		return fmt.Errorf("separator (%s) must be a string consisting of a single byte", separatorStr)
	}
	*s = separator(separatorStr[0])
	return nil
}

type persistManagerConfiguration struct {
	FilePathPrefix     *string        `yaml:"filePathPrefix"`
	WriteBufferSize    *int           `yaml:"writeBufferSize"`
	RawDocSourceField  *string        `yaml:"rawDocSourceField"`
	TimestampPrecision *time.Duration `yaml:"timestampPrecision"`
}

func (c *persistManagerConfiguration) NewPersistManager(
	filePathSeparator byte,
	timestampFieldName string,
) persist.Manager {
	opts := fs.NewOptions().
		SetFieldPathSeparator(filePathSeparator).
		SetTimestampField(timestampFieldName)
	if c.FilePathPrefix != nil {
		opts = opts.SetFilePathPrefix(*c.FilePathPrefix)
	}
	if c.WriteBufferSize != nil {
		opts = opts.SetWriteBufferSize(*c.WriteBufferSize)
	}
	if c.RawDocSourceField != nil {
		opts = opts.SetRawDocSourceField(*c.RawDocSourceField)
	}
	if c.TimestampPrecision != nil {
		opts = opts.SetTimestampPrecision(*c.TimestampPrecision)
	}
	return fs.NewPersistManager(opts)
}
