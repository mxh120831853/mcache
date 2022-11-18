package cache

import (
	"context"
	"math/rand"
	"sync"
	"time"
	"unsafe"
)

const (
	DefaultCheckSecond = 60
)

type cacheItem struct {
	expireSec  int
	expireTime time.Time
	value      interface{}
}

type cacheKV struct {
	k string
	v *cacheItem
}

type LocalCache struct {
	expireSec int
	r         *rand.Rand
	m         sync.Mutex
	cache     map[string]interface{}
	expireFn  CacheExpireFunc
}

type CacheExpireFunc func(key string, value interface{})

type LocalOption func(c *LocalCache)

func LocalWithExpire(expireSecond int) LocalOption {
	return func(c *LocalCache) {
		c.expireSec = expireSecond
	}
}

func LocalExpireNotify(fn CacheExpireFunc) LocalOption {
	return func(c *LocalCache) {
		c.expireFn = fn
	}
}

func NewLocalCache(ctx context.Context, opts ...LocalOption) *Cache {
	c := &LocalCache{
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
		cache: map[string]interface{}{},
	}
	for _, fn := range opts {
		fn(c)
	}
	go c.runExpireCheck(ctx)
	return NewCache(c)
}

func (c *LocalCache) Set(key string, value interface{}) error {
	exp := time.Time{}
	if c.expireSec != 0 {
		exp = time.Now().Add(time.Second * time.Duration(c.expireSec+c.r.Intn(int(c.expireSec/10+1))))
	}
	data := &cacheItem{
		expireSec:  c.expireSec,
		expireTime: exp,
		value:      value,
	}
	c.m.Lock()
	c.cache[key] = data
	c.m.Unlock()
	return nil
}

func (c *LocalCache) SetWithExpire(key string, value interface{}, expireSec int) error {
	exp := time.Time{}
	if expireSec != 0 {
		exp = time.Now().Add(time.Second * time.Duration(expireSec+c.r.Intn(int(expireSec/10+1))))
	}
	data := &cacheItem{
		expireSec:  expireSec,
		expireTime: exp,
		value:      value,
	}
	c.m.Lock()
	c.cache[key] = data
	c.m.Unlock()
	return nil
}

func (c *LocalCache) Get(key string) (interface{}, error) {
	c.m.Lock()
	defer c.m.Unlock()
	value, ok := c.cache[key]
	if !ok {
		return nil, nil
	}
	data, ok := value.(*cacheItem)
	if !ok {
		return nil, ErrDataType
	}
	if data.expireSec != 0 {
		data.expireTime = time.Now().Add(time.Duration(data.expireSec)*time.Second + time.Duration(c.r.Intn(int(data.expireSec/10+1))))
	}
	return data.value, nil
}

func (c *LocalCache) GetInt(key string) (*int64, error) {
	value, err := c.Get(key)
	if value == nil {
		return nil, err
	}
	var ret int64
	switch v := value.(type) {
	case int:
		ret = int64(v)
	case int8:
		ret = int64(v)
	case int16:
		ret = int64(v)
	case int32:
		ret = int64(v)
	case int64:
		ret = int64(v)
	case uint:
		ret = int64(v)
	case uint8:
		ret = int64(v)
	case uint16:
		ret = int64(v)
	case uint32:
		ret = int64(v)
	default:
		return nil, ErrDataType
	}
	return &ret, nil
}

func (c *LocalCache) GetFloat(key string) (*float64, error) {
	value, err := c.Get(key)
	if value == nil {
		return nil, err
	}
	var ret float64
	switch v := value.(type) {
	case float32:
		ret = float64(v)
	case float64:
		ret = float64(v)
	default:
		return nil, ErrDataType
	}
	return &ret, nil
}

func (c *LocalCache) GetString(key string) (string, error) {
	value, err := c.Get(key)
	if value == nil {
		return "", err
	}
	var ret string
	switch v := value.(type) {
	case string:
		ret = v
	case []byte:
		ret = *(*string)(unsafe.Pointer(&v))
	default:
		return "", ErrDataType
	}
	return ret, nil
}

func (c *LocalCache) GetBytes(key string) ([]byte, error) {
	value, err := c.Get(key)
	if value == nil {
		return nil, err
	}
	var ret []byte
	switch v := value.(type) {
	case string:
		ret = []byte(v)
	case []byte:
		ret = v
	default:
		return nil, ErrDataType
	}
	return ret, nil
}

func (c *LocalCache) GetBool(key string) (*bool, error) {
	value, err := c.Get(key)
	if value == nil {
		return nil, err
	}
	ret := false
	switch v := value.(type) {
	case float32, float64, int, int64:
		if v == 1 {
			ret = true
		}
	case string:
		if v == "true" || v == "1" || v == "t" || v == "T" {
			ret = true
		}
	case bool:
		ret = v
	default:
		return nil, ErrDataType
	}
	return &ret, nil
}

func (c *LocalCache) Del(key string) error {
	c.m.Lock()
	delete(c.cache, key)
	c.m.Unlock()
	return nil
}

func (c *LocalCache) runExpireCheck(ctx context.Context) {
	exp := c.expireSec
	if exp > 0 {
		exp /= 2
	} else {
		exp = DefaultCheckSecond
	}
	timer := time.NewTimer(time.Duration(exp) * time.Second)
	tmpDel := []*cacheKV{}
	for {
		select {
		case <-timer.C:
			c.m.Lock()
			for k, v := range c.cache {
				data, ok := v.(*cacheItem)
				if !ok {
					delete(c.cache, k)
					continue
				}
				if !data.expireTime.IsZero() && time.Now().After(data.expireTime) {
					delete(c.cache, k)
					tmpDel = append(tmpDel, &cacheKV{k: k, v: data})
				}
			}
			c.m.Unlock()
			for _, x := range tmpDel {
				if c.expireFn != nil {
					c.expireFn(x.k, x.v.value)
				}
			}
			tmpDel = tmpDel[0:0]
			timer = time.NewTimer(time.Duration(exp) * time.Second)
		case <-ctx.Done():
			return
		}
	}
}
