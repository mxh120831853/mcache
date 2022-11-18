package bloom

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

var pool *redigo.Pool

func getRedigoT(t *testing.T) GetRedisConn {
	return func() redigo.Conn {
		if pool == nil {
			pool = &redigo.Pool{
				MaxIdle:     3,
				IdleTimeout: 60 * time.Second,
				Dial: func() (redigo.Conn, error) {
					return redigo.Dial("tcp",
						redisAddr, redigo.DialPassword(redisPass))
				},
				TestOnBorrow: func(c redigo.Conn, t time.Time) error {
					_, err := c.Do("PING")
					return err
				},
			}
		}
		c, err := pool.GetContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
}

func getRedigoB(b *testing.B) GetRedisConn {
	return func() redigo.Conn {
		if pool == nil {
			pool = &redigo.Pool{
				MaxIdle:     3,
				IdleTimeout: 60 * time.Second,
				Dial: func() (redigo.Conn, error) {
					return redigo.Dial("tcp",
						redisAddr, redigo.DialPassword(redisPass))
				},
				TestOnBorrow: func(c redigo.Conn, t time.Time) error {
					_, err := c.Do("PING")
					return err
				},
			}
		}
		c, err := pool.GetContext(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		return c
	}
}

func TestRedigoConcurrent(t *testing.T) {
	gmp := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(gmp)

	f := NewRedisgo(1000, 4, "test:123", getRedigoT(t))
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

func TestRedigoBasic(t *testing.T) {
	f := NewRedisgo(1000, 4, "test:123", getRedigoT(t))
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

func TestRedigoBasicUint32(t *testing.T) {
	f := NewRedisgo(1000, 4, "test:123", getRedigoT(t))
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

func TestRedigoNewWithLowNumbers(t *testing.T) {
	f := NewRedisgo(0, 0, "test:123", getRedigoT(t))
	defer f.ClearAll()
	if f.K() != 1 {
		t.Errorf("%v should be 1", f.K())
	}
	if f.b.M() != 1 {
		t.Errorf("%v should be 1", f.b.M())
	}
}

func TestRedigoString(t *testing.T) {
	f := NewRedisgoWithEstimates(1000, 0.001, "test:123", getRedigoT(t))
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

func testRedigoEstimated(n uint, maxFp float64, t *testing.T) {
	m, k := EstimateParameters(n, maxFp)
	f := NewRedisgoWithEstimates(n, maxFp, "test:123", getRedigoT(t))
	defer f.ClearAll()
	fpRate := f.EstimateFalsePositiveRate(n)
	if fpRate > 1.5*maxFp {
		t.Errorf("False positive rate too high: n: %v; m: %v; k: %v; maxFp: %f; fpRate: %f, fpRate/maxFp: %f", n, m, k, maxFp, fpRate, fpRate/maxFp)
	}
}

func TestRedigoEstimated1000_0001(t *testing.T)   { testRedigoEstimated(1000, 0.000100, t) }
func TestRedigoEstimated10000_0001(t *testing.T)  { testRedigoEstimated(10000, 0.000100, t) }
func TestRedigoEstimated100000_0001(t *testing.T) { testRedigoEstimated(100000, 0.000100, t) }

func TestRedigoEstimated1000_001(t *testing.T)   { testRedigoEstimated(1000, 0.001000, t) }
func TestRedigoEstimated10000_001(t *testing.T)  { testRedigoEstimated(10000, 0.001000, t) }
func TestRedigoEstimated100000_001(t *testing.T) { testRedigoEstimated(100000, 0.001000, t) }

func TestRedigoEstimated1000_01(t *testing.T)   { testRedigoEstimated(1000, 0.010000, t) }
func TestRedigoEstimated10000_01(t *testing.T)  { testRedigoEstimated(10000, 0.010000, t) }
func TestRedigoEstimated100000_01(t *testing.T) { testRedigoEstimated(100000, 0.010000, t) }

func TestRedigoCap(t *testing.T) {
	f := NewRedisgo(1000, 4, "test:123", getRedigoT(t))
	defer f.ClearAll()
	if f.Cap() != f.b.M() {
		t.Error("not accessing Cap() correctly")
	}
}

func BenchmarkRedigoEstimated(b *testing.B) {
	c := getRedigoB(b)
	for n := uint(100000); n <= 100000; n *= 10 {
		for fp := 0.1; fp >= 0.0001; fp /= 10.0 {
			f := NewRedisgoWithEstimates(n, fp, "test:123", c)
			f.EstimateFalsePositiveRate(n)
			f.ClearAll()
		}
	}
}

func BenchmarkRedigoSeparateTestAndAdd(b *testing.B) {
	f := NewRedisgoWithEstimates(uint(b.N), 0.0001, "test:123", getRedigoB(b))
	defer f.ClearAll()
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.Test(key)
		f.Add(key)
	}
}

func BenchmarkRedigoCombinedTestAndAdd(b *testing.B) {
	f := NewRedisgoWithEstimates(uint(b.N), 0.0001, "test:123", getRedigoB(b))
	defer f.ClearAll()
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.TestAndAdd(key)
	}
}

func TestRedigoFPP(t *testing.T) {
	f := NewRedisgoWithEstimates(1000, 0.001, "test:123", getRedigoT(t))
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
