package bloom

import (
	"sync"

	"github.com/bits-and-blooms/bitset"
)

type LocalBloom struct {
	mtx sync.Mutex
	k   uint
	b   *bitset.BitSet
}

func NewLocal(m, k uint) *BloomFilter {
	lb := &LocalBloom{
		k: max(1, k),
		b: bitset.New(max(1, m)),
	}
	return NewBloom(lb)
}

func NewLocalWithEstimates(n uint, fp float64) *BloomFilter {
	m, k := EstimateParameters(n, fp)
	return NewLocal(m, k)
}

func (l *LocalBloom) K() uint {
	l.mtx.Lock()
	k := l.k
	l.mtx.Unlock()
	return k
}

func (l *LocalBloom) M() uint {
	l.mtx.Lock()
	m := l.b.Len()
	l.mtx.Unlock()
	return m
}

func (l *LocalBloom) SetAll(h [4]uint64) error {
	l.mtx.Lock()
	for i := uint(0); i < l.k; i++ {
		loc := uint(location(h, i) % uint64(l.b.Len()))
		l.b.Set(loc)
	}
	l.mtx.Unlock()
	return nil
}

func (l *LocalBloom) TestAll(h [4]uint64) (bool, error) {
	l.mtx.Lock()
	for i := uint(0); i < l.k; i++ {
		loc := uint(location(h, i) % uint64(l.b.Len()))
		if !l.b.Test(loc) {
			l.mtx.Unlock()
			return false, nil
		}
	}
	l.mtx.Unlock()
	return true, nil
}

func (l *LocalBloom) TestAddAll(h [4]uint64) (bool, error) {
	present := true
	l.mtx.Lock()
	for i := uint(0); i < l.k; i++ {
		loc := uint(location(h, i) % uint64(l.b.Len()))
		if !l.b.Test(loc) {
			present = false
		}
		l.b.Set(loc)
	}
	l.mtx.Unlock()
	return present, nil
}

func (l *LocalBloom) ClearAll() error {
	l.mtx.Lock()
	l.b.ClearAll()
	l.mtx.Unlock()
	return nil
}
