// Copyright 2013, Sébastien Paolacci. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package murmur3 implements Austin Appleby's non-cryptographic MurmurHash3.

 Reference implementation:
    http://code.google.com/p/smhasher/wiki/MurmurHash3

 History, characteristics and (legacy) perfs:
    https://sites.google.com/site/murmurhash/
    https://sites.google.com/site/murmurhash/statistics
*/
package murmur3

type digest struct {
	clen int      // Digested input cumulative length.
	buf  [16]byte // Expected (but not required) to be Size() large.
	seed uint32   // Seed for initializing the hash.
	tail int      // Length used in buf to store tailing unprocessed bytes.
}

func (d digest) BlockSize() int {
	return 1
}

func (d digest) loadUint32(idx int) uint32 {
	b := idx * 4
	return uint32(d.buf[b+0]) | uint32(d.buf[b+1])<<8 | uint32(d.buf[b+2])<<16 | uint32(d.buf[b+3])<<24
}

func (d digest) loadUint64(idx int) uint64 {
	b := idx * 8
	return uint64(d.buf[b+0]) | uint64(d.buf[b+1])<<8 | uint64(d.buf[b+2])<<16 | uint64(d.buf[b+3])<<24 |
		uint64(d.buf[b+4])<<32 | uint64(d.buf[b+5])<<40 | uint64(d.buf[b+6])<<48 | uint64(d.buf[b+7])<<56
}
