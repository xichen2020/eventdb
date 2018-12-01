package fs

import (
	"os"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	// defaultWriterBufferSize is the default buffer size for writing files.
	defaultWriterBufferSize = 65536
)

var (
	// Default prefix to the directory where the segment files are persisted.
	defaultFilePathPrefix = os.TempDir()

	// Default file mode when creating new files.
	defaultNewFileMode = os.FileMode(0666)

	// Default file mode when creating new directories.
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)

	// Default separator used when persisting and querying nested fields.
	defaultFieldPathSeparator = byte('.')
)

// Options provide a set of options for data persistence.
type Options struct {
	clockOpts          clock.Options
	instrumentOpts     instrument.Options
	filePathPrefix     string
	newFileMode        os.FileMode
	newDirectoryMode   os.FileMode
	writeBufferSize    int
	fieldPathSeparator byte
}

// NewOptions provide a new set of options.
func NewOptions() *Options {
	return &Options{
		clockOpts:          clock.NewOptions(),
		instrumentOpts:     instrument.NewOptions(),
		filePathPrefix:     defaultFilePathPrefix,
		newFileMode:        defaultNewFileMode,
		newDirectoryMode:   defaultNewDirectoryMode,
		writeBufferSize:    defaultWriterBufferSize,
		fieldPathSeparator: defaultFieldPathSeparator,
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
