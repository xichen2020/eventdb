// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package query

import (
	"bytes"

	"github.com/m3db/m3/src/x/pool"

	"github.com/cespare/xxhash"
)

// BytesResultArrayHashMapOptions provides options used when created the map.
type BytesResultArrayHashMapOptions struct {
	InitialSize int
	KeyCopyPool pool.BytesPool
}

// NewBytesResultArrayHashMap returns a new byte keyed map.
func NewBytesResultArrayHashMap(opts BytesResultArrayHashMapOptions) *BytesResultArrayHashMap {
	var (
		copyFn     BytesCopyFunc
		finalizeFn BytesFinalizeFunc
	)
	if pool := opts.KeyCopyPool; pool == nil {
		copyFn = func(k []byte) []byte {
			return append([]byte(nil), k...)
		}
	} else {
		copyFn = func(k []byte) []byte {
			keyLen := len(k)
			pooled := pool.Get(keyLen)[:keyLen]
			copy(pooled, k)
			return pooled
		}
		finalizeFn = func(k []byte) {
			pool.Put(k)
		}
	}
	return bytesResultArrayHashAlloc(bytesResultArrayHashOptions{
		hash: func(k []byte) BytesResultArrayHashMapHash {
			return BytesResultArrayHashMapHash(xxhash.Sum64(k))
		},
		equals:      bytes.Equal,
		copy:        copyFn,
		finalize:    finalizeFn,
		initialSize: opts.InitialSize,
	})
}
