package fs

import (
	"os"
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	// Default file mode when creating new files.
	defaultNewFileMode = os.FileMode(0666)

	// Default file mode when creating new directories.
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)

	// Default separator used when persisting and querying nested fields.
	defaultFieldPathSeparator = byte('.')

	// Default timestamp precision.
	defaultTimestampPrecision = time.Millisecond

	// Default write buffer size.
	defaultWriteBufferSize = 65536

	// Default read buffer size.
	defaultReadBufferSize = 65536

	// defaultMmapEnableHugePages is the default setting whether to enable huge pages or not.
	defaultMmapEnableHugePages = false

	// defaultMmapHugePagesThreshold is the default threshold for when to enable huge pages if enabled.
	defaultMmapHugePagesThreshold = int64(2 << 14) // 16kb (or when eclipsing 4 pages of default 4096 page size)
)

var (
	// Default prefix to the directory where the segment files are persisted.
	defaultFilePathPrefix = os.TempDir()
)

// Options provide a set of options for data persistence.
type Options struct {
	clockOpts              clock.Options
	instrumentOpts         instrument.Options
	filePathPrefix         string
	newFileMode            os.FileMode
	newDirectoryMode       os.FileMode
	fieldPathSeparator     byte
	timestampPrecision     time.Duration
	readBufferSize         int
	writeBufferSize        int
	mmapEnableHugePages    bool
	mmapHugePagesThreshold int64
}

// NewOptions provide a new set of options.
func NewOptions() *Options {
	return &Options{
		clockOpts:              clock.NewOptions(),
		instrumentOpts:         instrument.NewOptions(),
		filePathPrefix:         defaultFilePathPrefix,
		newFileMode:            defaultNewFileMode,
		newDirectoryMode:       defaultNewDirectoryMode,
		fieldPathSeparator:     defaultFieldPathSeparator,
		timestampPrecision:     defaultTimestampPrecision,
		readBufferSize:         defaultReadBufferSize,
		writeBufferSize:        defaultWriteBufferSize,
		mmapEnableHugePages:    defaultMmapEnableHugePages,
		mmapHugePagesThreshold: defaultMmapHugePagesThreshold,
	}
}

// SetClockOptions sets the clock options.
func (o *Options) SetClockOptions(v clock.Options) *Options {
	opts := *o
	opts.clockOpts = v
	return &opts
}

// ClockOptions returns the clock options.
func (o *Options) ClockOptions() clock.Options {
	return o.clockOpts
}

// SetInstrumentOptions sets the instrument options.
func (o *Options) SetInstrumentOptions(v instrument.Options) *Options {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *Options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

// SetFilePathPrefix sets the file path prefix for persisted dataset.
func (o *Options) SetFilePathPrefix(v string) *Options {
	opts := *o
	opts.filePathPrefix = v
	return &opts
}

// FilePathPrefix returns the file path prefix for persisted dataset.
func (o *Options) FilePathPrefix() string {
	return o.filePathPrefix
}

// SetNewFileMode sets the new file mode.
func (o *Options) SetNewFileMode(v os.FileMode) *Options {
	opts := *o
	opts.newFileMode = v
	return &opts
}

// NewFileMode returns the new file mode.
func (o *Options) NewFileMode() os.FileMode {
	return o.newFileMode
}

// SetNewDirectoryMode sets the new directory mode.
func (o *Options) SetNewDirectoryMode(v os.FileMode) *Options {
	opts := *o
	opts.newDirectoryMode = v
	return &opts
}

// NewDirectoryMode returns the new directory mode.
func (o *Options) NewDirectoryMode() os.FileMode {
	return o.newDirectoryMode
}

// SetFieldPathSeparator sets the field separator.
func (o *Options) SetFieldPathSeparator(v byte) *Options {
	opts := *o
	opts.fieldPathSeparator = v
	return &opts
}

// FieldPathSeparator returns the field separator.
func (o *Options) FieldPathSeparator() byte {
	return o.fieldPathSeparator
}

// SetTimestampPrecision sets the timestamp precision.
func (o *Options) SetTimestampPrecision(v time.Duration) *Options {
	opts := *o
	opts.timestampPrecision = v
	return &opts
}

// TimestampPrecision returns the timestamp precision.
func (o *Options) TimestampPrecision() time.Duration {
	return o.timestampPrecision
}

// SetWriteBufferSize sets the buffer size for writing data to files.
func (o *Options) SetWriteBufferSize(v int) *Options {
	opts := *o
	opts.writeBufferSize = v
	return &opts
}

// WriteBufferSize returns the buffer size for writing data to files.
func (o *Options) WriteBufferSize() int {
	return o.writeBufferSize
}

// SetReadBufferSize sets the buffer size for reading data from files.
func (o *Options) SetReadBufferSize(v int) *Options {
	opts := *o
	opts.readBufferSize = v
	return &opts
}

// ReadBufferSize returns the buffer size for reading data from files.
func (o *Options) ReadBufferSize() int {
	return o.readBufferSize
}

// SetMmapEnableHugePages sets whether to enable huge pages or not.
func (o *Options) SetMmapEnableHugePages(v bool) *Options {
	opts := *o
	opts.mmapEnableHugePages = v
	return &opts
}

// MmapEnableHugePages returns whether to enable huge pages or not.
func (o *Options) MmapEnableHugePages() bool {
	return o.mmapEnableHugePages
}

// SetMmapHugePagesThreshold sets the threshold for when to enable huge pages if enabled.
func (o *Options) SetMmapHugePagesThreshold(v int64) *Options {
	opts := *o
	opts.mmapHugePagesThreshold = v
	return &opts
}

// MmapHugePagesThreshold returns the threshold for when to enable huge pages if enabled.
func (o *Options) MmapHugePagesThreshold() int64 {
	return o.mmapHugePagesThreshold
}
