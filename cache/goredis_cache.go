package mcache

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

const (
	getCacheStr string = `
	local key = KEYS[1]
	local value = redis.call('hget', key, 'data')
	local expire = redis.call('hget', key, 'exp')
	if (value ~= false) and (tonumber(expire) ~= 0)
	then
		redis.call('expire', key, expire)
	end
	return value
	`

	setCacheStr string = `
	local key,value,expire = KEYS[1],ARGV[1],ARGV[2]
	redis.call('hmset', key, 'data', value, 'exp', expire)
	if tonumber(expire) ~= 0
	then
		redis.call('expire', key, expire)
	end
	`
)

var (
	luaGetCache = redis.NewScript(getCacheStr)
	luaSetCache = redis.NewScript(setCacheStr)
)

type GoredisCache struct {
	expireSec int
	client    redis.UniversalClient
	r         *rand.Rand
}

type GoredisOption func(c *GoredisCache)

func GoredisWithExpire(expireSecond int) GoredisOption {
	return func(c *GoredisCache) {
		c.expireSec = expireSecond
	}
}

func NewGoredisCache(client redis.UniversalClient, opts ...GoredisOption) *Cache {
	c := &GoredisCache{
		client: client,
		r:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, fn := range opts {
		fn(c)
	}
	return NewCache(c)
}

func (c *GoredisCache) Set(key string, value interface{}) error {
	if c.client == nil {
		return ErrNoRedis
	}
	exp := c.expireSec
	if exp != 0 {
		exp += c.r.Intn(int(exp/10 + 1))
	}
	return luaSetCache.Run(c.client, []string{key}, value, exp).Err()
}

func (c *GoredisCache) SetWithExpire(key string, value interface{}, expireSec int) error {
	if c.client == nil {
		return ErrNoRedis
	}
	return luaSetCache.Run(c.client, []string{key}, value, expireSec).Err()
}

func (c *GoredisCache) Get(key string) (interface{}, error) {
	if c.client == nil {
		return nil, ErrNoRedis
	}
	value, err := luaGetCache.Run(c.client, []string{key}).Result()
	if err == redis.Nil || (value == nil && err == nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tmp, ok := value.(string)
	if !ok {
		return nil, ErrDataType
	}
	return tmp, err
}

func (c *GoredisCache) GetInt(key string) (*int64, error) {
	value, err := c.Get(key)
	if value == nil {
		return nil, err
	}
	data, err := strconv.ParseInt(value.(string), 10, 64)
	return &data, err
}

func (c *GoredisCache) GetFloat(key string) (*float64, error) {
	value, err := c.Get(key)
	if value == nil {
		return nil, err
	}
	data, err := strconv.ParseFloat(value.(string), 64)
	return &data, err
}
func (c *GoredisCache) GetString(key string) (string, error) {
	value, err := c.Get(key)
	if value == nil {
		return "", err
	}
	return value.(string), err
}
func (c *GoredisCache) GetBytes(key string) ([]byte, error) {
	data, err := c.GetString(key)
	if err != nil {
		return nil, err
	}
	return []byte(data), err
}
func (c *GoredisCache) GetBool(key string) (*bool, error) {
	value, err := c.Get(key)
	if value == nil {
		return nil, err
	}
	data, err := strconv.ParseBool(value.(string))
	return &data, err
}

func (c *GoredisCache) Del(key string) error {
	if c.client == nil {
		return ErrNoRedis
	}
	err := c.client.Del(key).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}
