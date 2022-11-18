package cache

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

var (
	redisAddr string = "192.168.3.105:6379"
	redisPass string = "test_123456"
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

func TestGoredisSet(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
	v := 3
	c.Set("test:123", v)
	data, _ := c.Get("test:123")
	if data == nil {
		t.Errorf("%v value error", data)
		return
	}
	if value, ok := data.(string); !ok {
		t.Errorf("%v value error", data)
		return
	} else {
		ret, _ := strconv.Atoi(value)
		if ret != v {
			t.Errorf("%v value error", ret)
			return
		}
	}
}

func TestGoredisSetInt(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
	v := 3
	c.Set("test:123", v)
	data, _ := c.GetInt("test:123")
	if data == nil || *data != int64(v) {
		t.Errorf("%v value error", data)
		return
	}
}

func TestGoredisSetFloat(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
	v := 3.0
	c.Set("test:123", v)
	data, _ := c.GetFloat("test:123")
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestGoredisSetString(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
	v := "test"
	c.Set("test:123", v)
	data, _ := c.GetString("test:123")
	if data == "" || data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestGoredisSetBytes(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
	v := []byte("test")
	c.Set("test:123", v)
	data, _ := c.GetBytes("test:123")
	if data == nil || !bytes.Equal(v, data) {
		t.Errorf("%v value error", data)
		return
	}
}

func TestGoredisSetBool(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
	v := true
	c.Set("test:123", v)
	data, _ := c.GetBool("test:123")
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestGoredisDel(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
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

func TestGoredisExpire(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
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

func TestGoredisExtend(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
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

func TestGoredisSetBoolNoExpire(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t))
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

func TestGoredisSetExpire(t *testing.T) {
	c := NewGoredisCache(getGoRedisT(t), GoredisWithExpire(10))
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
