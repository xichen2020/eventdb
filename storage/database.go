package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/sharding"
	"github.com/xichen2020/eventdb/x/hash"

	xerrors "github.com/m3db/m3x/errors"
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

var (
	// errDatabaseNotOpenOrClosed raised when trying to perform an action that requires
	// the databse is open.
	errDatabaseNotOpenOrClosed = errors.New("database is not open")

	// errDatabaseOpenOrClosed raised when trying to perform an action that requires
	// the database is not open.
	errDatabaseOpenOrClosed = errors.New("database is open or closed")
)

type databaseState int

const (
	databaseNotOpen databaseState = iota
	databaseOpen
	databaseClosed
)

type db struct {
	sync.RWMutex

	shardSet sharding.ShardSet
	opts     *Options

	state      databaseState
	namespaces map[hash.Hash]databaseNamespace
}

// NewDatabase creates a new database.
// NB: This assumes all namespaces share the same shardset.
// TODO(xichen): Add metrics.
func NewDatabase(
	namespaces [][]byte,
	shardSet sharding.ShardSet,
	opts *Options,
) Database {
	if opts == nil {
		opts = NewOptions()
	}
	nss := make(map[hash.Hash]databaseNamespace, len(namespaces))
	for _, ns := range namespaces {
		h := hash.BytesHash(ns)
		nss[h] = newDatabaseNamespace(ns, shardSet, opts)
	}
	return &db{
		opts:       opts,
		shardSet:   shardSet,
		namespaces: nss,
	}
}

// TODO(xichen): Start ticking.
func (d *db) Open() error {
	d.Lock()
	defer d.Unlock()

	if d.state != databaseNotOpen {
		return errDatabaseOpenOrClosed
	}
	d.state = databaseOpen
	return nil
}

func (d *db) Write(
	namespace []byte,
	ev event.Event,
) error {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		return err
	}
	return n.Write(ev)
}

func (d *db) Close() error {
	d.Lock()
	defer d.Unlock()

	if d.state != databaseOpen {
		return errDatabaseNotOpenOrClosed
	}
	d.state = databaseClosed

	// Close database-level resources.

	// Close namespaces.
	var multiErr xerrors.MultiError
	for _, ns := range d.ownedNamespacesWithLock() {
		if err := ns.Close(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (d *db) namespaceFor(namespace []byte) (databaseNamespace, error) {
	h := hash.BytesHash(namespace)
	d.RLock()
	n, exists := d.namespaces[h]
	d.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no such namespace %s", namespace)
	}
	return n, nil
}

// ownedNamespacesWithLock returns the list of owned namespaces within a lock.
// This ensures the internal list of namespaces may not be modified by the caller.
func (d *db) ownedNamespacesWithLock() []databaseNamespace {
	namespaces := make([]databaseNamespace, 0, len(d.namespaces))
	for _, n := range d.namespaces {
		namespaces = append(namespaces, n)
	}
	return namespaces
}
