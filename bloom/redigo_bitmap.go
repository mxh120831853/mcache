package bloom

import (
	redigo "github.com/gomodule/redigo/redis"
)

var redigoSetAll = redigo.NewScript(1, setAllStr)
var redigoTestAll = redigo.NewScript(1, testAllStr)
var redigoSetAddAll = redigo.NewScript(1, setAddAllStr)

type GetRedisConn func() redigo.Conn

type RedigoBloom struct {
	k       uint
	m       uint
	key     string
	getConn GetRedisConn
}

func NewRedisgo(m, k uint, redisKey string, getConn GetRedisConn) *BloomFilter {
	rb := &RedigoBloom{
		k:       max(1, k),
		m:       max(1, m),
		key:     redisKey,
		getConn: getConn,
	}
	return NewBloom(rb)
}

func NewRedisgoWithEstimates(n uint, fp float64, redisKey string, getConn GetRedisConn) *BloomFilter {
	m, k := EstimateParameters(n, fp)
	return NewRedisgo(m, k, redisKey, getConn)
}

func (l *RedigoBloom) K() uint {
	return l.k
}

func (l *RedigoBloom) M() uint {
	return l.m
}

func (l *RedigoBloom) SetAll(h [4]uint64) error {
	c := l.getConn()
	if c == nil {
		return ErrNoRedis
	}
	_, err := redigoSetAll.Do(c, l.key, l.k, l.m, uint32(h[0]), uint32(h[1]), uint32(h[2]), uint32(h[3]))
	c.Close()
	return err
}

func (l *RedigoBloom) TestAll(h [4]uint64) (bool, error) {
	c := l.getConn()
	if c == nil {
		return false, ErrNoRedis
	}
	ret, err := redigo.Int64(redigoTestAll.Do(c, l.key, l.k, l.m, uint32(h[0]), uint32(h[1]), uint32(h[2]), uint32(h[3])))
	if err != nil {
		c.Close()
		return false, err
	}
	if ret == 1 {
		c.Close()
		return true, nil
	}
	c.Close()
	return false, nil
}

func (l *RedigoBloom) TestAddAll(h [4]uint64) (bool, error) {
	c := l.getConn()
	if c == nil {
		return false, ErrNoRedis
	}
	ret, err := redigo.Int64(redigoSetAddAll.Do(c, l.key, l.k, l.m, uint32(h[0]), uint32(h[1]), uint32(h[2]), uint32(h[3])))
	if err != nil {
		c.Close()
		return false, err
	}
	if ret == 1 {
		c.Close()
		return true, nil
	}
	c.Close()
	return false, nil
}

func (l *RedigoBloom) ClearAll() error {
	c := l.getConn()
	if c == nil {
		return ErrNoRedis
	}
	_, err := c.Do("DEL", l.key)
	c.Close()
	return err
}
