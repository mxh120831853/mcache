package cache

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestLocalSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
	v := 3
	c.Set("test:123", v)
	data, _ := c.Get("test:123")
	if data == nil {
		t.Errorf("%v value error", data)
		return
	}
	if value, ok := data.(int); !ok {
		t.Errorf("%v value error", data)
		return
	} else {
		if value != v {
			t.Errorf("%v value error", value)
			return
		}
	}
}

func TestLocalSetInt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
	v := 3
	c.Set("test:123", v)
	data, _ := c.GetInt("test:123")
	if data == nil || *data != int64(v) {
		t.Errorf("%v value error", data)
		return
	}
}

func TestLocalSetFloat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
	v := 3.0
	c.Set("test:123", v)
	data, _ := c.GetFloat("test:123")
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestLocalSetString(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
	v := "test"
	c.Set("test:123", v)
	data, _ := c.GetString("test:123")
	if data == "" || data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestLocalSetBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
	v := []byte("test")
	c.Set("test:123", v)
	data, _ := c.GetBytes("test:123")
	if data == nil || !bytes.Equal(v, data) {
		t.Errorf("%v value error", data)
		return
	}
}

func TestLocalSetBool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
	v := true
	c.Set("test:123", v)
	data, _ := c.GetBool("test:123")
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
}

func TestLocalDel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
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

func TestLocalExpire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
	v := true
	key := "test:123"
	c.Set(key, v)
	data, _ := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	time.Sleep(20 * time.Second)
	data, err := c.GetBool(key)
	if data != nil || err != nil {
		t.Errorf("%v value error:%v", data, err)
		return
	}
}

func TestLocalExtend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(10))
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

func TestLocalSetBoolNoExpire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx)
	key := "test:123"
	v := true
	c.Set(key, v)
	data, _ := c.GetBool(key)
	if data == nil || *data != v {
		t.Errorf("%v value error", data)
		return
	}
	time.Sleep(300 * time.Second)
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

func TestLocalSetExpire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewLocalCache(ctx, LocalWithExpire(5))
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
	time.Sleep(45 * time.Second)
	data, err := c.GetBool(key)
	if data != nil || err != nil {
		t.Errorf("%v value error:%v", data, err)
		return
	}
}
