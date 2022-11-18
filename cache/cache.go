package mcache

import (
	"errors"
)

var (
	ErrNoRedis  = errors.New("no redis client error")
	ErrDataType = errors.New("data type error")
)

type ICache interface {
	Set(key string, value interface{}) error
	SetWithExpire(key string, value interface{}, expireSec int) error
	Get(key string) (interface{}, error)
	GetInt(key string) (*int64, error)
	GetFloat(key string) (*float64, error)
	GetString(key string) (string, error)
	GetBytes(key string) ([]byte, error)
	GetBool(key string) (*bool, error)
	Del(key string) error
}

type Cache struct {
	cache ICache
}

func NewCache(c ICache) *Cache {
	return &Cache{cache: c}
}

func (c *Cache) Set(key string, value interface{}) error {
	return c.cache.Set(key, value)
}

func (c *Cache) SetWithExpire(key string, value interface{}, expireSec int) error {
	return c.cache.SetWithExpire(key, value, expireSec)
}

func (c *Cache) Get(key string) (interface{}, error) {
	return c.cache.Get(key)
}

func (c *Cache) GetInt(key string) (*int64, error) {
	return c.cache.GetInt(key)
}

func (c *Cache) GetFloat(key string) (*float64, error) {
	return c.cache.GetFloat(key)
}

func (c *Cache) GetBool(key string) (*bool, error) {
	return c.cache.GetBool(key)
}

func (c *Cache) GetString(key string) (string, error) {
	return c.cache.GetString(key)
}

func (c *Cache) GetBytes(key string) ([]byte, error) {
	return c.cache.GetBytes(key)
}

func (c *Cache) Del(key string) error {
	return c.cache.Del(key)
}
