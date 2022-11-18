/*
Package bloom provides data structures and methods for creating Bloom filters.

A Bloom filter is a representation of a set of _n_ items, where the main
requirement is to make membership queries; _i.e._, whether an item is a
member of a set.

A Bloom filter has two parameters: _m_, a maximum size (typically a reasonably large
multiple of the cardinality of the set to represent) and _k_, the number of hashing
functions on elements of the set. (The actual hashing functions are important, too,
but this is not a parameter for this implementation). A Bloom filter is backed by
a BitSet; a key is represented in the filter by setting the bits at each value of the
hashing functions (modulo _m_). Set membership is done by _testing_ whether the
bits at each value of the hashing functions (again, modulo _m_) are set. If so,
the item is in the set. If the item is actually in the set, a Bloom filter will
never fail (the true positive rate is 1.0); but it is susceptible to false
positives. The art is to choose _k_ and _m_ correctly.

In this implementation, the hashing functions used is murmurhash,
a non-cryptographic hashing function.

This implementation accepts keys for setting as testing as []byte. Thus, to
add a string item, "Love":

    uint n = 1000
    filter := bloom.New(20*n, 5) // load of 20, 5 keys
    filter.Add([]byte("Love"))

Similarly, to test if "Love" is in bloom:

    if filter.Test([]byte("Love"))

For numeric data, I recommend that you look into the binary/encoding library. But,
for example, to add a uint32 to the filter:

    i := uint32(100)
    n1 := make([]byte,4)
    binary.BigEndian.PutUint32(n1,i)
    f.Add(n1)

Finally, there is a method to estimate the false positive rate of a particular
Bloom filter for a set of size _n_:

    if filter.EstimateFalsePositiveRate(1000) > 0.001

Given the particular hashing scheme, it's best to be empirical about this. Note
that estimating the FP rate will clear the Bloom filter.
*/
package bloom

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

var (
	ErrDataType = errors.New("result data type error")
	ErrNoRedis  = errors.New("no redis client error")
)

type BitMap interface {
	M() uint
	K() uint

	SetAll(h [4]uint64) error
	TestAll(h [4]uint64) (bool, error)
	TestAddAll(h [4]uint64) (bool, error)
	ClearAll() error
}

// A BloomFilter is a representation of a set of _n_ items, where the main
// requirement is to make membership queries; _i.e._, whether an item is a
// member of a set.
type BloomFilter struct {
	b BitMap
}

func max(x, y uint) uint {
	if x > y {
		return x
	}
	return y
}

// NewBloom creates a NewBloom Bloom filter with _m_ bits and _k_ hashing functions
// We force _m_ and _k_ to be at least one to avoid panics.
func NewBloom(b BitMap) *BloomFilter {
	return &BloomFilter{b}
}

// baseHashes returns the four hash values of data that are used to create k
// hashes
func baseHashes(data []byte) [4]uint64 {
	var d digest128 // murmur hashing
	hash1, hash2, hash3, hash4 := d.sum256(data)
	return [4]uint64{
		hash1, hash2, hash3, hash4,
	}
}

// location returns the ith hashed location using the four base hash values
func location(h [4]uint64, i uint) uint64 {
	ii := uint64(i)
	return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
}

// EstimateParameters estimates requirements for m and k.
// Based on https://bitbucket.org/ww/bloom/src/829aa19d01d9/bloom.go
// used with permission.
func EstimateParameters(n uint, p float64) (m uint, k uint) {
	m = uint(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = uint(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return
}

// Cap returns the capacity, _m_, of a Bloom filter
func (f *BloomFilter) Cap() uint {
	return f.b.M()
}

// K returns the number of hash functions used in the BloomFilter
func (f *BloomFilter) K() uint {
	return f.b.K()
}

// Add data to the Bloom Filter. Returns the filter (allows chaining)
func (f *BloomFilter) Add(data []byte) error {
	h := baseHashes(data)
	return f.b.SetAll(h)
}

// AddString to the Bloom Filter. Returns the filter (allows chaining)
func (f *BloomFilter) AddString(data string) error {
	return f.Add([]byte(data))
}

// Test returns true if the data is in the BloomFilter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (f *BloomFilter) Test(data []byte) (bool, error) {
	h := baseHashes(data)
	return f.b.TestAll(h)
}

// TestString returns true if the string is in the BloomFilter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (f *BloomFilter) TestString(data string) (bool, error) {
	return f.Test([]byte(data))
}

// TestAndAdd is the equivalent to calling Test(data) then Add(data).
// Returns the result of Test.
func (f *BloomFilter) TestAndAdd(data []byte) (bool, error) {
	h := baseHashes(data)
	return f.b.TestAddAll(h)
}

// TestAndAddString is the equivalent to calling Test(string) then Add(string).
// Returns the result of Test.
func (f *BloomFilter) TestAndAddString(data string) (bool, error) {
	return f.TestAndAdd([]byte(data))
}

// ClearAll clears all the data in a Bloom filter, removing all keys
func (f *BloomFilter) ClearAll() error {
	return f.b.ClearAll()
}

// EstimateFalsePositiveRate returns, for a BloomFilter with a estimate of m bits
// and k hash functions, what the false positive rate will be
// while storing n entries; runs 100,000 tests. This is an empirical
// test using integers as keys. As a side-effect, it clears the BloomFilter.
func (f *BloomFilter) EstimateFalsePositiveRate(n uint) (fpRate float64) {
	rounds := uint32(100000)
	concChan := make(chan struct{}, 1000)
	f.ClearAll()
	wg := sync.WaitGroup{}
	for i := uint32(0); i < uint32(n); i++ {
		concChan <- struct{}{}
		wg.Add(1)
		go func(ii uint32) {
			n1 := make([]byte, 4)
			binary.BigEndian.PutUint32(n1, ii)
			f.Add(n1)
			<-concChan
			wg.Done()
		}(i)
	}
	wg.Wait()
	fp := int32(0)
	// test for number of rounds
	for i := uint32(0); i < rounds; i++ {
		concChan <- struct{}{}
		wg.Add(1)
		go func(ii uint32) {
			n1 := make([]byte, 4)
			binary.BigEndian.PutUint32(n1, ii+uint32(n)+1)
			if r, _ := f.Test(n1); r {
				//fmt.Printf("%v failed.\n", i+uint32(n)+1)
				atomic.AddInt32(&fp, 1)
			}
			<-concChan
			wg.Done()
		}(i)
	}
	wg.Wait()
	fpRate = float64(fp) / (float64(rounds))
	f.ClearAll()
	return
}
