package bloom

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/go-redis/redis"
)

var (
	redisAddr string = "10.12.30.15:20002"
	redisPass string = "Test_12316"
)

func getGoRedisT(t *testing.T) redis.UniversalClient {
	c := redis.NewClient(
		&redis.Options{
			Addr:     redisAddr,
			Password: redisPass,
		})

	_, err := c.Ping().Result()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func getGoRedisB(b *testing.B) redis.UniversalClient {
	c := redis.NewClient(
		&redis.Options{
			Addr:     redisAddr,
			Password: redisPass,
		})

	_, err := c.Ping().Result()
	if err != nil {
		b.Fatal(err)
	}
	return c
}

func TestGoredisConcurrent(t *testing.T) {
	gmp := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(gmp)
	f := NewGoredis(1000, 4, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	f.Add(n1)
	f.Add(n2)

	var wg sync.WaitGroup
	const try = 1000
	var err1, err2 error

	wg.Add(1)
	go func() {
		for i := 0; i < try; i++ {
			n1b, _ := f.Test(n1)
			if !n1b {
				err1 = fmt.Errorf("%v should be in", n1)
				break
			}
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < try; i++ {
			n2b, _ := f.Test(n2)
			if !n2b {
				err2 = fmt.Errorf("%v should be in", n2)
				break
			}
		}
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		t.Fatal(err1)
	}
	if err2 != nil {
		t.Fatal(err2)
	}
}

func TestGoredisBasic(t *testing.T) {
	f := NewGoredis(1000, 4, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	n3 := []byte("Emma")
	f.Add(n1)
	n3a, _ := f.TestAndAdd(n3)
	n1b, _ := f.Test(n1)
	n2b, _ := f.Test(n2)
	n3b, _ := f.Test(n3)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
}

func TestGoredisBasicUint32(t *testing.T) {
	f := NewGoredis(1000, 4, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	n1 := make([]byte, 4)
	n2 := make([]byte, 4)
	n3 := make([]byte, 4)
	n4 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, 100)
	binary.BigEndian.PutUint32(n2, 101)
	binary.BigEndian.PutUint32(n3, 102)
	binary.BigEndian.PutUint32(n4, 103)
	f.Add(n1)
	n3a, _ := f.TestAndAdd(n3)
	n1b, _ := f.Test(n1)
	n2b, _ := f.Test(n2)
	n3b, _ := f.Test(n3)
	f.Test(n4)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
}

func TestGoredisNewWithLowNumbers(t *testing.T) {
	f := NewGoredis(0, 0, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	if f.K() != 1 {
		t.Errorf("%v should be 1", f.K())
	}
	if f.b.M() != 1 {
		t.Errorf("%v should be 1", f.b.M())
	}
}

func TestGoredisString(t *testing.T) {
	f := NewGoredisWithEstimates(1000, 0.001, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	n1 := "Love"
	n2 := "is"
	n3 := "in"
	n4 := "bloom"
	f.AddString(n1)
	n3a, _ := f.TestAndAddString(n3)
	n1b, _ := f.TestString(n1)
	n2b, _ := f.TestString(n2)
	n3b, _ := f.TestString(n3)
	f.TestString(n4)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}

}

func testGoredisEstimated(n uint, maxFp float64, t *testing.T) {
	m, k := EstimateParameters(n, maxFp)
	f := NewGoredisWithEstimates(n, maxFp, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	fpRate := f.EstimateFalsePositiveRate(n)
	if fpRate > 1.5*maxFp {
		t.Errorf("False positive rate too high: n: %v; m: %v; k: %v; maxFp: %f; fpRate: %f, fpRate/maxFp: %f", n, m, k, maxFp, fpRate, fpRate/maxFp)
	}
}

func TestGoredisEstimated1000_0001(t *testing.T)   { testGoredisEstimated(1000, 0.000100, t) }
func TestGoredisEstimated10000_0001(t *testing.T)  { testGoredisEstimated(10000, 0.000100, t) }
func TestGoredisEstimated100000_0001(t *testing.T) { testGoredisEstimated(100000, 0.000100, t) }

func TestGoredisEstimated1000_001(t *testing.T)   { testGoredisEstimated(1000, 0.001000, t) }
func TestGoredisEstimated10000_001(t *testing.T)  { testGoredisEstimated(10000, 0.001000, t) }
func TestGoredisEstimated100000_001(t *testing.T) { testGoredisEstimated(100000, 0.001000, t) }

func TestGoredisEstimated1000_01(t *testing.T)   { testGoredisEstimated(1000, 0.010000, t) }
func TestGoredisEstimated10000_01(t *testing.T)  { testGoredisEstimated(10000, 0.010000, t) }
func TestGoredisEstimated100000_01(t *testing.T) { testGoredisEstimated(100000, 0.010000, t) }

func TestGoredisCap(t *testing.T) {
	f := NewGoredis(1000, 4, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	if f.Cap() != f.b.M() {
		t.Error("not accessing Cap() correctly")
	}
}

func BenchmarkGoredisEstimated(b *testing.B) {
	c := getGoRedisB(b)
	for n := uint(100000); n <= 100000; n *= 10 {
		for fp := 0.1; fp >= 0.0001; fp /= 10.0 {
			f := NewGoredisWithEstimates(n, fp, "test:123", c)
			f.EstimateFalsePositiveRate(n)
			f.ClearAll()
		}
	}
}

func BenchmarkGoredisSeparateTestAndAdd(b *testing.B) {
	f := NewGoredisWithEstimates(uint(b.N), 0.0001, "test:123", getGoRedisB(b))
	defer f.ClearAll()
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.Test(key)
		f.Add(key)
	}
}

func BenchmarkGoredisCombinedTestAndAdd(b *testing.B) {
	f := NewGoredisWithEstimates(uint(b.N), 0.0001, "test:123", getGoRedisB(b))
	defer f.ClearAll()
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.TestAndAdd(key)
	}
}

func TestGoredisFPP(t *testing.T) {
	f := NewGoredisWithEstimates(1000, 0.001, "test:123", getGoRedisT(t))
	defer f.ClearAll()
	for i := uint32(0); i < 1000; i++ {
		n := make([]byte, 4)
		binary.BigEndian.PutUint32(n, i)
		f.Add(n)
	}
	count := 0

	for i := uint32(0); i < 1000; i++ {
		n := make([]byte, 4)
		binary.BigEndian.PutUint32(n, i+1000)
		if r, _ := f.Test(n); r {
			count += 1
		}
	}
	if float64(count)/1000.0 > 0.001 {
		t.Errorf("Excessive fpp")
	}
}
