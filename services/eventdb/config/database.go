package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/xichen2020/eventdb/persist/fs"
	"github.com/xichen2020/eventdb/storage"
	"github.com/xichen2020/eventdb/x/hash"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/m3db/m3x/instrument"
)

var (
	errNoNamespaceConfig      = errors.New("no namespace configuration provided")
	errNoPersistManagerConfig = errors.New("no persist manager configuration provided")
)

// DatabaseConfiguration provides database configuration.
type DatabaseConfiguration struct {
	Namespaces                  []namespaceConfiguration                      `yaml:"namespaces"`
	FilePathPrefix              *string                                       `yaml:"filePathPrefix"`
	FieldPathSeparator          *separator                                    `yaml:"fieldPathSeparator"`
	NamespaceFieldName          *string                                       `yaml:"namespaceFieldName"`
	TimestampFieldName          *string                                       `yaml:"timestampFieldName"`
	RawDocSourceFieldName       *string                                       `yaml:"rawDocSourceFieldName"`
	TickMinInterval             *time.Duration                                `yaml:"tickMinInterval"`
	SegmentUnloadAfterUnreadFor *time.Duration                                `yaml:"segmentUnloadAfterUnreadFor"`
	PersistManager              *persistManagerConfiguration                  `yaml:"persist"`
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

// NewOptions create a new set of database options from configuration.
func (c *DatabaseConfiguration) NewOptions(instrumentOpts instrument.Options) (*storage.Options, error) {
	if c.PersistManager == nil {
		return nil, errNoPersistManagerConfig
	}

	opts := storage.NewOptions().
		SetInstrumentOptions(instrumentOpts)
	if c.FilePathPrefix != nil {
		opts = opts.SetFilePathPrefix(*c.FilePathPrefix)
	}
	if c.FieldPathSeparator != nil {
		sepByte := byte(*c.FieldPathSeparator)
		fieldHashFn := func(fieldPath []string) hash.Hash {
			return hash.StringArrayHash(fieldPath, sepByte)
		}
		opts = opts.SetFieldPathSeparator(sepByte).SetFieldHashFn(fieldHashFn)
	}
	if c.NamespaceFieldName != nil {
		opts = opts.SetNamespaceFieldName(*c.NamespaceFieldName)
	}
	if c.TimestampFieldName != nil {
		opts = opts.SetTimestampFieldPath([]string{*c.TimestampFieldName})
	}
	if c.RawDocSourceFieldName != nil {
		opts = opts.SetRawDocSourceFieldPath([]string{*c.RawDocSourceFieldName})
	}
	if c.TickMinInterval != nil {
		opts = opts.SetTickMinInterval(*c.TickMinInterval)
	}
	if c.SegmentUnloadAfterUnreadFor != nil {
		opts = opts.SetSegmentUnloadAfterUnreadFor(*c.SegmentUnloadAfterUnreadFor)
	}

	scope := instrumentOpts.MetricsScope()
	fsOpts := c.PersistManager.NewFileSystemOptions(
		opts.FilePathPrefix(),
		opts.FieldPathSeparator(),
	)
	persistManager := fs.NewPersistManager(fsOpts.SetInstrumentOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("persist-manager")),
	))
	fieldRetriever := fs.NewFieldRetriever(fsOpts.SetInstrumentOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("field-retriever")),
	))
	opts = opts.SetPersistManager(persistManager).SetFieldRetriever(fieldRetriever)

	// Initialize various pools.
	if c.BoolArrayPool != nil {
		buckets := c.BoolArrayPool.NewBuckets()
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("bool-array-pool"))
		poolOpts := c.BoolArrayPool.NewPoolOptions(iOpts)
		boolArrayPool := pool.NewBucketizedBoolArrayPool(buckets, poolOpts)
		boolArrayPool.Init(func(capacity int) []bool { return make([]bool, 0, capacity) })
		opts = opts.SetBoolArrayPool(boolArrayPool)
	}
	if c.IntArrayPool != nil {
		buckets := c.IntArrayPool.NewBuckets()
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("int-array-pool"))
		poolOpts := c.IntArrayPool.NewPoolOptions(iOpts)
		intArrayPool := pool.NewBucketizedIntArrayPool(buckets, poolOpts)
		intArrayPool.Init(func(capacity int) []int { return make([]int, 0, capacity) })
		opts = opts.SetIntArrayPool(intArrayPool)
	}
	if c.Int64ArrayPool != nil {
		buckets := c.Int64ArrayPool.NewBuckets()
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("int64-array-pool"))
		poolOpts := c.Int64ArrayPool.NewPoolOptions(iOpts)
		int64ArrayPool := pool.NewBucketizedInt64ArrayPool(buckets, poolOpts)
		int64ArrayPool.Init(func(capacity int) []int64 { return make([]int64, 0, capacity) })
		opts = opts.SetInt64ArrayPool(int64ArrayPool)
	}
	if c.DoubleArrayPool != nil {
		buckets := c.DoubleArrayPool.NewBuckets()
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("double-array-pool"))
		poolOpts := c.DoubleArrayPool.NewPoolOptions(iOpts)
		doubleArrayPool := pool.NewBucketizedFloat64ArrayPool(buckets, poolOpts)
		doubleArrayPool.Init(func(capacity int) []float64 { return make([]float64, 0, capacity) })
		opts = opts.SetDoubleArrayPool(doubleArrayPool)
	}
	if c.StringArrayPool != nil {
		buckets := c.StringArrayPool.NewBuckets()
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("string-array-pool"))
		poolOpts := c.StringArrayPool.NewPoolOptions(iOpts)
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
	WriteBufferSize        *int           `yaml:"writeBufferSize"`
	ReadBufferSize         *int           `yaml:"readBufferSize"`
	TimestampPrecision     *time.Duration `yaml:"timestampPrecision"`
	MmapEnableHugePages    *bool          `yaml:"mmapEnableHugePages"`
	MmapHugePagesThreshold *int64         `yaml:"mmapHugePagesThreshold"`
}

func (c *persistManagerConfiguration) NewFileSystemOptions(
	filePathPrefix string,
	fieldPathSeparator byte,
) *fs.Options {
	opts := fs.NewOptions().
		SetFilePathPrefix(filePathPrefix).
		SetFieldPathSeparator(fieldPathSeparator)
	if c.WriteBufferSize != nil {
		opts = opts.SetWriteBufferSize(*c.WriteBufferSize)
	}
	if c.ReadBufferSize != nil {
		opts = opts.SetReadBufferSize(*c.ReadBufferSize)
	}
	if c.TimestampPrecision != nil {
		opts = opts.SetTimestampPrecision(*c.TimestampPrecision)
	}
	if c.MmapEnableHugePages != nil {
		opts = opts.SetMmapEnableHugePages(*c.MmapEnableHugePages)
	}
	if c.MmapHugePagesThreshold != nil {
		opts = opts.SetMmapHugePagesThreshold(*c.MmapHugePagesThreshold)
	}
	return opts
}
