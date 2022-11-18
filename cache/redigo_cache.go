package cache

import (
	"math/rand"
	"strconv"
	"time"
	"unsafe"

	redigo "github.com/gomodule/redigo/redis"
)

var (
	redigoGetCache = redigo.NewScript(1, getCacheStr)
	redigoSetCache = redigo.NewScript(1, setCacheStr)
)

type GetRedisConn func() redigo.Conn

type RedigoCache struct {
	expireSec int
	getConn   GetRedisConn
	rnd       *rand.Rand
}

type RedigoOption func(c *RedigoCache)

func RedigoWithExpire(expireSecond int) RedigoOption {
	return func(c *RedigoCache) {
		c.expireSec = expireSecond
	}
}

func NewRedigoCache(getConn GetRedisConn, opts ...RedigoOption) *Cache {
	c := &RedigoCache{
		getConn: getConn,
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, fn := range opts {
		fn(c)
	}
	return NewCache(c)
}

func (r *RedigoCache) Set(key string, value interface{}) error {
	c := r.getConn()
	if c == nil {
		return ErrNoRedis
	}
	exp := r.expireSec
	if exp > 0 {
		exp += r.rnd.Intn(int(exp/10 + 1))
	}
	_, err := redigoSetCache.Do(c, key, value, exp)
	return err
}

func (r *RedigoCache) SetWithExpire(key string, value interface{}, expireSec int) error {
	c := r.getConn()
	if c == nil {
		return ErrNoRedis
	}
	_, err := redigoSetCache.Do(c, key, value, expireSec)
	return err
}

func (r *RedigoCache) Get(key string) (interface{}, error) {
	c := r.getConn()
	if c == nil {
		return nil, ErrNoRedis
	}
	value, err := redigoGetCache.Do(c, key, r.expireSec)
	if err == redigo.ErrNil || (value == nil && err == nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tmp, ok := value.([]byte)
	if !ok {
		return nil, ErrDataType
	}
	return tmp, err
}

func (r *RedigoCache) GetInt(key string) (*int64, error) {
	value, err := r.Get(key)
	if value == nil {
		return nil, err
	}
	data, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	return &data, err
}

func (r *RedigoCache) GetFloat(key string) (*float64, error) {
	value, err := r.Get(key)
	if value == nil {
		return nil, err
	}
	data, err := strconv.ParseFloat(string(value.([]byte)), 64)
	return &data, err
}

func (r *RedigoCache) GetString(key string) (string, error) {
	value, err := r.Get(key)
	if value == nil {
		return "", err
	}
	v := value.([]byte)
	return *(*string)(unsafe.Pointer(&v)), err
}

func (r *RedigoCache) GetBytes(key string) ([]byte, error) {
	value, err := r.Get(key)
	if value == nil {
		return nil, err
	}

	return value.([]byte), err
}

func (r *RedigoCache) GetBool(key string) (*bool, error) {
	value, err := r.Get(key)
	if value == nil {
		return nil, err
	}
	data, err := strconv.ParseBool(string(value.([]byte)))
	return &data, err
}

func (r *RedigoCache) Del(key string) error {
	c := r.getConn()
	if c == nil {
		return ErrNoRedis
	}
	_, err := c.Do("DEL", key)
	if err == redigo.ErrNil {
		return nil
	}
	return err
}
