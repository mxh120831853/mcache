# mcache

support redis bloom cache, use redigo or goredis or local cache

based on <https://github.com/bits-and-blooms/bloom>

usage

```go
package main

import (
 "mcache/cache"

 "github.com/go-redis/redis"
)

func main() {
 c := redis.NewClient(
  &redis.Options{
   Addr:     "192.168.3.105:6379",
   Password: "test_123456",
  })

 _, err := c.Ping().Result()
 if err != nil {
  println(err)
  return
 }
 cache := cache.NewGoredisCache(c, cache.GoredisWithExpire(10))
 v := 3
 cache.Set("test:123", v)
 data, _ := cache.Get("test:123")
 println(data)
}
```
