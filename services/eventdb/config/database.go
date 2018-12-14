package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/persist/fs"
	"github.com/xichen2020/eventdb/sharding"
	"github.com/xichen2020/eventdb/storage"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/context"
	xpool "github.com/m3db/m3x/pool"
	"github.com/uber-go/tally"
)

var (
	errNoNamespaceConfig      = errors.New("no namespace configuration provided")
	errNoPersistManagerConfig = errors.New("no persist manager configuration provided")
)

// DatabaseConfiguration provides database configuration.
type DatabaseConfiguration struct {
	Namespaces                  []namespaceConfiguration                      `yaml:"namespaces"`
	NumShards                   int                                           `yaml:"numShards"`
	FilePathPrefix              *string                                       `yaml:"filePathPrefix"`
	FieldPathSeparator          *separator                                    `yaml:"fieldPathSeparator"`
	NamespaceFieldName          *string                                       `yaml:"namespaceFieldName"`
	TimestampFieldName          *string                                       `yaml:"timestampFieldName"`
	TickMinInterval             *time.Duration                                `yaml:"tickMinInterval"`
	MaxNumDocsPerSegment        *int32                                        `yaml:"maxNumDocsPerSegment"`
	SegmentUnloadAfterUnreadFor *time.Duration                                `yaml:"segmentUnloadAfterUnreadFor"`
	PersistManager              *persistManagerConfiguration                  `yaml:"persist"`
	ContextPool                 *contextPoolConfiguration                     `yaml:"contextPool"`
	BoolArrayPool               *pool.BucketizedBoolArrayPoolConfiguration    `yaml:"boolArrayPool"`
	IntArrayPool                *pool.BucketizedIntArrayPoolConfiguration     `yaml:"intArrayPool"`
	Int64ArrayPool              *pool.BucketizedInt64ArrayPoolConfiguration   `yaml:"int64ArrayPool"`
	DoubleArrayPool             *pool.BucketizedFloat64ArrayPoolConfiguration `yaml:"doubleArrayPool"`
	StringArrayPool             *pool.BucketizedStringArrayPoolConfiguration  `yaml:"stringArrayPool"`
}

// NewNamespacesMetadata creates metadata for namespaces.
func (c *DatabaseConfiguration) NewNamespacesMetadata() ([]storage.NamespaceMetadata, error) {
	if len(c.Namespaces) == 0 {
		return nil, errNoNamespaceConfig
	}
	// Configure namespace metadata.
	namespaces := make([]storage.NamespaceMetadata, 0, len(c.Namespaces))
	for _, nsconfig := range c.Namespaces {
		ns := nsconfig.NewMetadata()
		namespaces = append(namespaces, ns)
	}
	return namespaces, nil
}

// NewShardSet creates a new shardset.
func (c *DatabaseConfiguration) NewShardSet() (sharding.ShardSet, error) {
	shardIDs := make([]uint32, 0, c.NumShards)
	for i := 0; i < c.NumShards; i++ {
		shardIDs = append(shardIDs, uint32(i))
	}
	shards := sharding.NewShards(shardIDs, shard.Available)
	hashFn := sharding.DefaultHashFn(c.NumShards)
	return sharding.NewShardSet(shards, hashFn)
}

// NewOptions create a new set of database options from configuration.
func (c *DatabaseConfiguration) NewOptions(scope tally.Scope) (*storage.Options, error) {
	if c.PersistManager == nil {
		return nil, errNoPersistManagerConfig
	}

	opts := storage.NewOptions()
	if c.FilePathPrefix != nil {
		opts = opts.SetFilePathPrefix(*c.FilePathPrefix)
	}
	if c.FieldPathSeparator != nil {
		opts = opts.SetFieldPathSeparator(byte(*c.FieldPathSeparator))
	}
	if c.NamespaceFieldName != nil {
		opts = opts.SetNamespaceFieldName(*c.NamespaceFieldName)
	}
	if c.TimestampFieldName != nil {
		opts = opts.SetTimestampFieldName(*c.TimestampFieldName)
	}
	if c.TickMinInterval != nil {
		opts = opts.SetTickMinInterval(*c.TickMinInterval)
	}
	if c.MaxNumDocsPerSegment != nil {
		opts = opts.SetMaxNumDocsPerSegment(*c.MaxNumDocsPerSegment)
	}
	if c.SegmentUnloadAfterUnreadFor != nil {
		opts = opts.SetSegmentUnloadAfterUnreadFor(*c.SegmentUnloadAfterUnreadFor)
	}
	persistManager := c.PersistManager.NewPersistManager(
		opts.FilePathPrefix(),
		opts.FieldPathSeparator(),
		opts.TimestampFieldName(),
	)
	opts = opts.SetPersistManager(persistManager)

	// Initialize various pools.
	if c.ContextPool != nil {
		contextPool := c.ContextPool.NewContextPool()
		opts = opts.SetContextPool(contextPool)
	}
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

// namespaceConfiguration provides namespace configuration.
type namespaceConfiguration struct {
	ID        string         `yaml:"id" validate:"nonzero"`
	Retention *time.Duration `yaml:"retention"`
}

func (c *namespaceConfiguration) NewMetadata() storage.NamespaceMetadata {
	nsOpts := storage.NewNamespaceOptions()
	if c.Retention != nil {
		nsOpts = nsOpts.SetRetention(*c.Retention)
	}
	return storage.NewNamespaceMetadata([]byte(c.ID), nsOpts)
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
	WriteBufferSize    *int           `yaml:"writeBufferSize"`
	RawDocSourceField  *string        `yaml:"rawDocSourceField"`
	TimestampPrecision *time.Duration `yaml:"timestampPrecision"`
}

func (c *persistManagerConfiguration) NewPersistManager(
	filePathPrefix string,
	fieldPathSeparator byte,
	timestampFieldName string,
) persist.Manager {
	opts := fs.NewOptions().
		SetFilePathPrefix(filePathPrefix).
		SetFieldPathSeparator(fieldPathSeparator).
		SetTimestampField(timestampFieldName)
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

// contextPoolConfiguration provides the configuration for the context pool.
type contextPoolConfiguration struct {
	Size                *int     `yaml:"size"`
	RefillLowWaterMark  *float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`
	RefillHighWaterMark *float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`

	// The maximum allowable size for a slice of finalizers that the
	// pool will allow to be returned (finalizer slices that grow too
	// large during use will be discarded instead of returning to the
	// pool where they would consume more memory.)
	MaxFinalizerCapacity *int `yaml:"maxFinalizerCapacity" validate:"min=0"`
}

func (c *contextPoolConfiguration) NewContextPool() context.Pool {
	objPoolOpts := xpool.NewObjectPoolOptions()
	if c.Size != nil {
		objPoolOpts = objPoolOpts.SetSize(*c.Size)
	}
	if c.RefillLowWaterMark != nil {
		objPoolOpts = objPoolOpts.SetRefillLowWatermark(*c.RefillLowWaterMark)
	}
	if c.RefillHighWaterMark != nil {
		objPoolOpts = objPoolOpts.SetRefillHighWatermark(*c.RefillHighWaterMark)
	}
	opts := context.NewOptions().SetContextPoolOptions(objPoolOpts)
	if c.MaxFinalizerCapacity != nil {
		opts = opts.SetMaxPooledFinalizerCapacity(*c.MaxFinalizerCapacity)
	}
	return context.NewPool(opts)
}
