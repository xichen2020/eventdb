package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/sharding"

	xerrors "github.com/m3db/m3x/errors"
)

// databaseNamespace is a database namespace.
type databaseNamespace interface {
	// Write writes an event within the namespace.
	Write(ev event.Event) error

	// Flush performs a flush against the namespace.
	Flush(pm persist.Manager) error

	// Close closes the namespace.
	Close() error
}

var (
	errNamespaceAlreadyClosed = errors.New("namespace already closed")
)

type dbNamespace struct {
	sync.RWMutex

	id       []byte
	shardSet sharding.ShardSet
	opts     *Options

	closed bool
	shards []databaseShard
}

func newDatabaseNamespace(
	id []byte,
	shardSet sharding.ShardSet,
	opts *Options,
) *dbNamespace {
	idClone := make([]byte, len(id))
	copy(idClone, id)

	n := &dbNamespace{
		id:          id,
		shardSet:    shardSet,
		opts:        opts,
		tickWorkers: tickWorkers,
	}
	n.initShards()
	return n
}

func (n *dbNamespace) Write(ev event.Event) error {
	shard, err := n.shardFor(ev.ID)
	if err != nil {
		return err
	}
	return shard.Write(ev)
}

func (n *dbNamespace) Flush(pm persist.Manager) error {
	return fmt.Errorf("not implemented")
}

func (n *dbNamespace) Close() error {
	n.Lock()
	if n.closed {
		n.Unlock()
		return errNamespaceAlreadyClosed
	}
	n.closed = true
	shards := n.shards
	n.shards = shards[:0]
	n.shardSet = sharding.NewEmptyShardSet(sharding.DefaultHashFn(1))
	n.Unlock()
	n.closeShards(shards)
	return nil
}

func (n *dbNamespace) initShards() {
	n.Lock()
	defer n.Unlock()

	shards := n.shardSet.AllIDs()
	dbShards := make([]databaseShard, n.shardSet.Max()+1)
	for _, shard := range shards {
		dbShards[shard] = newDatabaseShard(shard, n.opts)
	}
	n.shards = dbShards
}

func (n *dbNamespace) shardFor(id []byte) (databaseShard, error) {
	n.RLock()
	shardID := n.shardSet.Lookup(id)
	shard, err := n.shardAtWithRLock(shardID)
	n.RUnlock()
	return shard, err
}

func (n *dbNamespace) shardAtWithRLock(shardID uint32) (databaseShard, error) {
	if int(shardID) >= len(n.shards) {
		return nil, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	shard := n.shards[shardID]
	if shard == nil {
		return nil, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	return shard, nil
}

// nolint:megacheck
func (n *dbNamespace) getOwnedShards() []databaseShard {
	n.RLock()
	shards := n.shardSet.AllIDs()
	databaseShards := make([]databaseShard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = n.shards[shard]
	}
	n.RUnlock()
	return databaseShards
}

func (n *dbNamespace) closeShards(shards []databaseShard) {
	var wg sync.WaitGroup
	closeFn := func(shard databaseShard) {
		shard.Close()
		wg.Done()
	}

	wg.Add(len(shards))
	for _, shard := range shards {
		dbShard := shard
		if dbShard == nil {
			continue
		}
		go closeFn(dbShard)
	}

	wg.Wait()
}
