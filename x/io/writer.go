package io

import "io"

// FlushableWriter embeds the `io.Writer` iface and also implements a Flush()
type FlushableWriter interface {
	io.Writer

	Flush() error
}
