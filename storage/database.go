package storage

import (
	"fmt"

	"github.com/xichen2020/eventdb/event"
)

// Database is a database for timestamped events.
// TODO(xichen): Add read APIs.
// TODO(xichen): Add batch write APIs.
type Database interface {
	// Open opens the database for reading and writing events.
	Open() error

	// Write writes a single timestamped event to a namespace.
	Write(namespace []byte, ev event.Event) error

	// Close closes the database.
	Close() error
}

type db struct {
	opts *Options
}

// New reutrns a new database instance.
func New(opts *Options) Database {
	return &db{
		opts: opts,
	}
}

func (d *db) Open() error {
	return fmt.Errorf("not implemented")
}

func (d *db) Write(
	namespace []byte,
	ev event.Event,
) error {
	return fmt.Errorf("not implemented")
}

func (d *db) Close() error {
	return fmt.Errorf("not implemented")
}
