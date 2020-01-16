// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package filter

import (
	"github.com/ccfarm/goleveldb/leveldb/util"
)

func bloomHash(key []byte) uint32 {
	return util.Hash(key, 0xbc9f1d34)
}

type bloomFilter int

// Name: The bloom filter serializes its parameters and is backward compatible
// with respect to them. Therefor, its parameters are not added to its
// name.
func (bloomFilter) Name() string {
	return "leveldb.BuiltinBloomFilter"
}

func (f bloomFilter) Contains(filter, key []byte) bool {
	nBytes := len(filter) - 1
	if nBytes < 1 {
		return false
	}
	nBits := uint32(nBytes * 8)

	// Use the encoded k so that we can read filters generated by
	// bloom filters created using different parameters.
	k := filter[nBytes]
	if k > 30 {
		// Reserved for potentially new encodings for short bloom filters.
		// Consider it a match.
		return true
	}

	kh := bloomHash(key)
	delta := (kh >> 17) | (kh << 15) // Rotate right 17 bits
	for j := uint8(0); j < k; j++ {
		bitpos := kh % nBits
		if (uint32(filter[bitpos/8]) & (1 << (bitpos % 8))) == 0 {
			return false
		}
		kh += delta
	}
	return true
}

func (f bloomFilter) NewGenerator() FilterGenerator {
	// Round down to reduce probing cost a little bit.
	k := uint8(f * 69 / 100) // 0.69 =~ ln(2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	return &bloomFilterGenerator{
		n: int(f),
		k: k,
	}
}

type bloomFilterGenerator struct {
	n int
	k uint8

	keyHashes []uint32
}

func (g *bloomFilterGenerator) Add(key []byte) {
	// Use double-hashing to generate a sequence of hash values.
	// See analysis in [Kirsch,Mitzenmacher 2006].
	g.keyHashes = append(g.keyHashes, bloomHash(key))
}

func (g *bloomFilterGenerator) Generate(b Buffer) {
	// Compute bloom filter size (in both bits and bytes)
	nBits := uint32(len(g.keyHashes) * g.n)
	// For small n, we can see a very high false positive rate.  Fix it
	// by enforcing a minimum bloom filter length.
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	dest := b.Alloc(int(nBytes) + 1)
	dest[nBytes] = g.k
	for _, kh := range g.keyHashes {
		delta := (kh >> 17) | (kh << 15) // Rotate right 17 bits
		for j := uint8(0); j < g.k; j++ {
			bitpos := kh % nBits
			dest[bitpos/8] |= (1 << (bitpos % 8))
			kh += delta
		}
	}

	g.keyHashes = g.keyHashes[:0]
}

// NewBloomFilter creates a new initialized bloom filter for given
// bitsPerKey.
//
// Since bitsPerKey is persisted individually for each bloom filter
// serialization, bloom filters are backwards compatible with respect to
// changing bitsPerKey. This means that no big performance penalty will
// be experienced when changing the parameter. See documentation for
// opt.Options.Filter for more information.
func NewBloomFilter(bitsPerKey int) Filter {
	return bloomFilter(bitsPerKey)
}
