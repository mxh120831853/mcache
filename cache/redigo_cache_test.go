package mcache

import (
	"bytes"
	"context"
	"strconv"
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
func TestRedigoSet(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := 3
	c.Set("test:123", v)
	data, _ := c.Get("test:123")
	if data == nil {
		t.Errorf("%v value error", data)
		return
	}
	if value, ok := data.([]byte); !ok {
		t.Errorf("%v value error", data)
		return
	} else {
		ret, _ := strconv.Atoi(string(value))
		if ret != v {
			t.Errorf("%v value error", ret)
			return
		}
	}
}

func TestRedigoSetInt(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := 3
	c.Set("test:123", v)
	data, _ := c.GetInt("test:123")
	if data == nil || *data != int64(v) {
		t.Errorf("%v value error", data)
		return
	}
}

func TestRedigoSetFloat(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := 3.0
	c.Set("test:123", v)
	data, _ := c.GetFloat("test:123")
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestRedigoSetString(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := "test"
	c.Set("test:123", v)
	data, _ := c.GetString("test:123")
	if data == "" || data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestRedigoSetBytes(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := []byte("test")
	c.Set("test:123", v)
	data, _ := c.GetBytes("test:123")
	if data == nil || !bytes.Equal(v, data) {
		t.Errorf("%v value error", data)
		return
	}
}

func TestRedigoSetBool(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := true
	c.Set("test:123", v)
	data, _ := c.GetBool("test:123")
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestRedigoDel(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := true
	key := "test:123"
	c.Set(key, v)
	data, _ := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	c.Del(key)
	data, err := c.GetBool(key)
	if data != nil || err != nil {
		t.Errorf("%v value error:%v", data, err)
		return
	}
}

func TestRedigoExpire(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := true
	key := "test:123"
	c.Set(key, v)
	data, _ := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	time.Sleep(15 * time.Second)
	data, err := c.GetBool(key)
	if data != nil || err != nil {
		t.Errorf("%v value error:%v", data, err)
		return
	}
}

func TestRedigoExtend(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := true
	key := "test:123"
	c.Set(key, v)
	data, _ := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	go func() {
		for i := 0; i < 2; i++ {
			time.Sleep(7 * time.Second)
			data, _ := c.GetBool(key)
			if data == nil || *data != v {
				t.Errorf("%v value error", data)
				return
			}
		}
	}()
	time.Sleep(15 * time.Second)
	data, err := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error:%v", data, err)
		return
	}
}

func TestRedigoSetBoolNoExpire(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t))
	key := "test:123"
	v := true
	c.Set(key, v)
	data, _ := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	time.Sleep(10 * time.Second)
	data, _ = c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	c.Del(key)
	data, err := c.GetBool(key)
	if data != nil || err != nil {
		t.Errorf("%v value error:%v", data, err)
		return
	}
}

func TestRedigoSetExpire(t *testing.T) {
	c := NewRedigoCache(getRedigoT(t), RedigoWithExpire(10))
	v := true
	key := "test:123"
	c.SetWithExpire(key, v, 30)
	data, _ := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	time.Sleep(15 * time.Second)
	data, _ = c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	time.Sleep(35 * time.Second)
	data, err := c.GetBool(key)
	if data != nil || err != nil {
		t.Errorf("%v value error:%v", data, err)
		return
	}
}
