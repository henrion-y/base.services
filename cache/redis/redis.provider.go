package redis

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/henrion-y/base.services/cache"
	"github.com/henrion-y/base.services/infra/redisapi"
)

func NewRedisProvider(redisApi *redisapi.RedisApi) (cache.Cache, error) {
	return &Cache{r: redisApi.RedisPool}, nil
}
