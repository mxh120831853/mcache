package bloom

import "github.com/go-redis/redis"

const (
	setAllStr string = `
	local bloom_key,k,m,h1,h2,h3,h4 = KEYS[1],ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6]
	local h = {h1,h2,h3,h4}
	for i=1,k do
		local ii = i-1
		local loc = (h[(ii%2)+1]+ii*h[3+(((ii+(ii%2))%4)/2)])%m
		redis.call('setbit', bloom_key, loc, 1)
	end
	`
	testAllStr string = `
	local bloom_key,k,m,h1,h2,h3,h4 = KEYS[1],ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6]
	local h = {h1,h2,h3,h4}
	for i=1,k do
		local ii = i-1
		local loc = (h[(ii%2)+1]+ii*h[3+(((ii+(ii%2))%4)/2)])%m
		if 0 == redis.call('getbit', bloom_key, loc)
		then
			return 0
		end
	end
	return 1
	`
	setAddAllStr string = `
	local bloom_key,k,m,h1,h2,h3,h4 = KEYS[1],ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6]
	local h = {h1,h2,h3,h4}
	local present = 1
	for i=1,k do
		local ii = i-1
		local loc = (h[(ii%2)+1]+ii*h[3+(((ii+(ii%2))%4)/2)])%m
		if 0 == redis.call('getbit', bloom_key, loc)
		then
			present = 0
		end
		redis.call('setbit', bloom_key, loc, 1)
	end
	return present
	`
)

var luaSetAll = redis.NewScript(setAllStr)
var luaTestAll = redis.NewScript(testAllStr)
var luaSetAddAll = redis.NewScript(setAddAllStr)

type GoredisBloom struct {
	k      uint
	m      uint
	key    string
	client redis.UniversalClient
}

func NewGoredis(m, k uint, redisKey string, client redis.UniversalClient) *BloomFilter {
	gb := &GoredisBloom{
		k:      max(1, k),
		m:      max(1, m),
		key:    redisKey,
		client: client,
	}
	return NewBloom(gb)
}

func NewGoredisWithEstimates(n uint, fp float64, redisKey string, client redis.UniversalClient) *BloomFilter {
	m, k := EstimateParameters(n, fp)
	return NewGoredis(m, k, redisKey, client)
}

func (l *GoredisBloom) K() uint {
	return l.k
}

func (l *GoredisBloom) M() uint {
	return l.m
}

func (l *GoredisBloom) SetAll(h [4]uint64) error {
	if l.client == nil {
		return ErrNoRedis
	}
	_, err := luaSetAll.Run(l.client, []string{l.key}, l.k, l.m, uint32(h[0]), uint32(h[1]), uint32(h[2]), uint32(h[3])).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (l *GoredisBloom) TestAll(h [4]uint64) (bool, error) {
	if l.client == nil {
		return false, ErrNoRedis
	}
	data, err := luaTestAll.Run(l.client, []string{l.key}, l.k, l.m, uint32(h[0]), uint32(h[1]), uint32(h[2]), uint32(h[3])).Result()
	if err != nil {
		return false, err
	}
	ret, ok := data.(int64)
	if !ok {
		return false, ErrDataType
	}
	if ret == 1 {
		return true, nil
	}
	return false, nil
}

func (l *GoredisBloom) TestAddAll(h [4]uint64) (bool, error) {
	if l.client == nil {
		return false, ErrNoRedis
	}
	data, err := luaSetAddAll.Run(l.client, []string{l.key}, l.k, l.m, uint32(h[0]), uint32(h[1]), uint32(h[2]), uint32(h[3])).Result()
	if err != nil {
		return false, err
	}
	ret, ok := data.(int64)
	if !ok {
		return false, ErrDataType
	}
	if ret == 1 {
		return true, nil
	}
	return false, nil
}

func (l *GoredisBloom) ClearAll() error {
	if l.client == nil {
		return ErrNoRedis
	}
	return l.client.Del(l.key).Err()
}
