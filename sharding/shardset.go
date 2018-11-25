package sharding

import (
	"errors"
	"math"

	"github.com/m3db/m3cluster/shard"

	"github.com/spaolacci/murmur3"
)

// HashGen generates HashFn based on the length of shards
type HashGen func(length int) HashFn

// HashFn is a sharding hash function.
type HashFn func(id []byte) uint32

// ShardSet contains a sharding function and a set of shards, this interface
// allows for potentially out of order shard sets.
type ShardSet interface {
	// All returns a slice to the shards in this set.
	All() []shard.Shard

	// AllIDs returns a slice to the shard IDs in this set.
	AllIDs() []uint32

	// Lookup will return a shard for a given identifier
	Lookup(id []byte) uint32

	// Min returns the smallest shard owned by this shard set
	Min() uint32

	// Max returns the largest shard owned by this shard set
	Max() uint32

	// HashFn returns the sharding hash function
	HashFn() HashFn
}

var (
	// ErrDuplicateShards returned when shard set is empty
	ErrDuplicateShards = errors.New("duplicate shards")

	// ErrInvalidShardID is returned on an invalid shard ID
	ErrInvalidShardID = errors.New("no shard with given ID")
)

type shardSet struct {
	shards   []shard.Shard
	ids      []uint32
	shardMap map[uint32]shard.Shard
	fn       HashFn
}

// NewShardSet creates a new sharding scheme with a set of shards
func NewShardSet(shards []shard.Shard, fn HashFn) (ShardSet, error) {
	if err := validateShards(shards); err != nil {
		return nil, err
	}
	return newValidatedShardSet(shards, fn), nil
}

// NewEmptyShardSet creates a new sharding scheme with an empty set of shards
func NewEmptyShardSet(fn HashFn) ShardSet {
	return newValidatedShardSet(nil, fn)
}

func newValidatedShardSet(shards []shard.Shard, fn HashFn) ShardSet {
	ids := make([]uint32, len(shards))
	shardMap := make(map[uint32]shard.Shard, len(shards))
	for i, shard := range shards {
		ids[i] = shard.ID()
		shardMap[shard.ID()] = shard
	}
	return &shardSet{
		shards:   shards,
		ids:      ids,
		shardMap: shardMap,
		fn:       fn,
	}
}

func (s *shardSet) Lookup(id []byte) uint32 {
	return s.fn(id)
}

func (s *shardSet) All() []shard.Shard {
	return s.shards[:]
}

func (s *shardSet) AllIDs() []uint32 {
	return s.ids[:]
}

func (s *shardSet) Min() uint32 {
	min := uint32(math.MaxUint32)
	for _, shard := range s.ids {
		if shard < min {
			min = shard
		}
	}
	return min
}

func (s *shardSet) Max() uint32 {
	max := uint32(0)
	for _, shard := range s.ids {
		if shard > max {
			max = shard
		}
	}
	return max
}

func (s *shardSet) HashFn() HashFn {
	return s.fn
}

// NewShards returns a new slice of shards with a specified state
func NewShards(ids []uint32, state shard.State) []shard.Shard {
	shards := make([]shard.Shard, len(ids))
	for i, id := range ids {
		shards[i] = shard.NewShard(id).SetState(state)
	}
	return shards
}

// IDs returns a new slice of shard IDs for a set of shards
func IDs(shards []shard.Shard) []uint32 {
	ids := make([]uint32, len(shards))
	for i := range ids {
		ids[i] = shards[i].ID()
	}
	return ids
}

func validateShards(shards []shard.Shard) error {
	uniqueShards := make(map[uint32]struct{}, len(shards))
	for _, s := range shards {
		if _, exist := uniqueShards[s.ID()]; exist {
			return ErrDuplicateShards
		}
		uniqueShards[s.ID()] = struct{}{}
	}
	return nil
}

// DefaultHashFn generates a HashFn based on murmur32
func DefaultHashFn(length int) HashFn {
	return NewHashFn(length, 0)
}

// NewHashGenWithSeed generates a HashFnGen based on murmur32 with a given seed
func NewHashGenWithSeed(seed uint32) HashGen {
	return func(length int) HashFn {
		return NewHashFn(length, seed)
	}
}

// NewHashFn generates a HashFN based on murmur32 with a given seed
func NewHashFn(length int, seed uint32) HashFn {
	return func(id []byte) uint32 {
		return murmur3.Sum32WithSeed(id, seed) % uint32(length)
	}
}
