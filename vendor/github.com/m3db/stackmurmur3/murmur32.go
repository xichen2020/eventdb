package murmur3

// http://code.google.com/p/guava-libraries/source/browse/guava/src/com/google/common/hash/Murmur3_32HashFunction.java

import (
	"fmt"
	"unsafe"
)

const (
	c1_32 uint32 = 0xcc9e2d51
	c2_32 uint32 = 0x1b873593
)

// Digest32 represents a partial evaluation of a 32 bits hash.
type Digest32 struct {
	digest
	h1 uint32 // Unfinalized running hash.
}

// New32 returns new 32-bit hasher
func New32() Digest32 { return New32WithSeed(0) }

// New32WithSeed returns new 32-bit hasher set with explicit seed value
func New32WithSeed(seed uint32) Digest32 {
	return Digest32{digest: digest{seed: seed}, h1: seed}
}

func (d Digest32) Size() int {
	return 4
}

func (d Digest32) Sum(b []byte) []byte {
	h := d.Sum32()
	return append(b, byte(h>>24), byte(h>>16), byte(h>>8), byte(h))
}

// Digest as many blocks as possible.
func (d Digest32) bmix(p []byte) (Digest32, []byte) {
	h1 := d.h1

	nblocks := len(p) / 4
	for i := 0; i < nblocks; i++ {
		k1 := *(*uint32)(unsafe.Pointer(&p[i*4]))

		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // rotl32(h1, 13)
		h1 = h1*4 + h1 + 0xe6546b64
	}
	d.h1 = h1
	return d, p[nblocks*d.Size():]
}

func (d Digest32) bmixbuf() Digest32 {
	h1 := d.h1

	if d.tail != d.Size() {
		panic(fmt.Errorf("expected full block"))
	}

	k1 := d.loadUint32(0)

	k1 *= c1_32
	k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
	k1 *= c2_32

	h1 ^= k1
	h1 = (h1 << 13) | (h1 >> 19) // rotl32(h1, 13)
	h1 = h1*4 + h1 + 0xe6546b64

	d.h1 = h1
	d.tail = 0
	return d
}

func (d Digest32) Sum32() (h1 uint32) {

	h1 = d.h1

	var k1 uint32
	switch d.tail & 3 {
	case 3:
		k1 ^= uint32(d.buf[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(d.buf[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(d.buf[0])
		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	h1 ^= uint32(d.clen)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

// Write will write bytes to the digest and return a new digest
// representing the derived digest.
func (d Digest32) Write(p []byte) Digest32 {
	n := len(p)
	d.clen += n

	if d.tail > 0 {
		// Stick back pending bytes.
		nfree := d.Size() - d.tail // nfree ∈ [1, d.size-1].
		if len(p) <= int(nfree) {
			// Everything can fit in buf
			for i := 0; i < len(p); i++ {
				d.buf[d.tail] = p[i]
				d.tail++
			}
			if len(p) == int(nfree) {
				d = d.bmixbuf()
			}
			return d
		}

		// One full block can be formed.
		add := p[:nfree]
		for i := 0; i < len(add); i++ {
			d.buf[d.tail] = add[i]
			d.tail++
		}

		// Process the full block
		d = d.bmixbuf()

		p = p[nfree:]
	}

	d, tail := d.bmix(p)

	// Keep own copy of the 0 to Size()-1 pending bytes.
	d.tail = len(tail)
	for i := 0; i < d.tail; i++ {
		d.buf[i] = tail[i]
	}

	return d
}

/*
func rotl32(x uint32, r byte) uint32 {
	return (x << r) | (x >> (32 - r))
}
*/

// Sum32 returns the MurmurHash3 sum of data. It is equivalent to the
// following sequence (without the extra burden and the extra allocation):
//     hasher := New32()
//     hasher.Write(data)
//     return hasher.Sum32()
func Sum32(data []byte) uint32 { return Sum32WithSeed(data, 0) }

// Sum32WithSeed returns the MurmurHash3 sum of data. It is equivalent to the
// following sequence (without the extra burden and the extra allocation):
//     hasher := New32WithSeed(seed)
//     hasher.Write(data)
//     return hasher.Sum32()
func Sum32WithSeed(data []byte, seed uint32) uint32 {
	h1 := seed

	nblocks := len(data) / 4
	var p uintptr
	if len(data) > 0 {
		p = uintptr(unsafe.Pointer(&data[0]))
	}
	p1 := p + uintptr(4*nblocks)
	for ; p < p1; p += 4 {
		k1 := *(*uint32)(unsafe.Pointer(p))

		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // rotl32(h1, 13)
		h1 = h1*4 + h1 + 0xe6546b64
	}

	tail := data[nblocks*4:]

	var k1 uint32
	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	h1 ^= uint32(len(data))

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}
