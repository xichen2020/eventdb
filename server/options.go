package server

import "time"

const (
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
)

// Options is a set of server options.
type Options interface {
	// SetReadTimeout sets the read timeout.
	SetReadTimeout(value time.Duration) Options

	// ReadTimeout returns the read timeout.
	ReadTimeout() time.Duration

	// SetWriteTimeout sets the write timeout.
	SetWriteTimeout(value time.Duration) Options

	// WriteTimeout returns the write timeout.
	WriteTimeout() time.Duration
}

type options struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewOptions creates a new set of server options.
func NewOptions() Options {
	return &options{
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}
}

func (o *options) SetReadTimeout(value time.Duration) Options {
	opts := *o
	opts.readTimeout = value
	return &opts
}

func (o *options) ReadTimeout() time.Duration {
	return o.readTimeout
}

func (o *options) SetWriteTimeout(value time.Duration) Options {
	opts := *o
	opts.writeTimeout = value
	return &opts
}

func (o *options) WriteTimeout() time.Duration {
	return o.writeTimeout
}
