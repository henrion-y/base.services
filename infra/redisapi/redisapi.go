package redisapi

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/henrion-y/base.services/infra/zlog"
	json "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"math"
	"math/rand"
	"time"

	"go.uber.org/zap"
)

type RedisApi struct {
	ServiceName string
	Client      *redis.Client
}

func NewRedisApiProvider(config *viper.Viper) (*RedisApi, error) {
	serviceName := config.GetString("redis.ServiceName")
	host := config.GetString("redis.Host")
	password := config.GetString("redis.Password")
	db := config.GetInt("redis.Db")
	readTimeout := config.GetInt("redis.ReadTimeout")
	writeTimeout := config.GetInt("redis.WriteTimeout")
	rdb := redis.NewClient(&redis.Options{
		Addr:         host,
		DB:           db,
		Password:     password,
		ReadTimeout:  time.Duration(readTimeout) * time.Second,
		WriteTimeout: time.Duration(writeTimeout) * time.Second,
	})

	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		return nil, err
	}
	return &RedisApi{
		ServiceName: serviceName,
		Client:      rdb,
	}, err
}

func (r *RedisApi) do(ctx context.Context, timeout time.Duration, fn func(ctx context.Context)) {
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	timeoutContext, cancelFunc := context.WithTimeout(ctx, timeout)
	defer cancelFunc()

	fn(timeoutContext)
}

func (r *RedisApi) Set(ctx context.Context, key string, value interface{}, expiration time.Duration, timeout time.Duration) (val string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.Set(ctx, key, value, expiration).Result()
		if err != nil {
			zlog.Error("Set err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("value", value),
				zap.Any("expiration", expiration),
				zap.Any("err", err))
		}
	})
	return val, err
}

// SetInterface 接受任意类型数据，先序列化转byte数组再存到数据库
func (r *RedisApi) SetInterface(ctx context.Context, key string, value interface{}, expiration time.Duration, timeout time.Duration) (val string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		var byteValue []byte
		byteValue, err = json.Marshal(value)
		if err != nil {
			zlog.Error("SetAndMarshal Marshal err",
				zap.Any("value", value),
				zap.Any("err", err),
			)
			return
		}

		val, err = r.Client.Set(ctx, key, byteValue, expiration).Result()
		if err != nil {
			zlog.Error("Set err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("value", value),
				zap.Any("expiration", expiration),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) SetNx(ctx context.Context, key string, value string, expiration time.Duration, timeout time.Duration) (val bool, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.SetNX(ctx, key, value, expiration).Result()
		if err != nil && err != redis.Nil {
			zlog.Info("SetNx err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.String("value", value))
		}
	})
	return val, err
}

func (r *RedisApi) SetInterfaceNx(ctx context.Context, key string, value interface{}, expiration time.Duration, timeout time.Duration) (val bool, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		var byteValue []byte
		byteValue, err = json.Marshal(value)
		if err != nil {
			zlog.Error("SetInterfaceNx Marshal err",
				zap.Any("value", value),
				zap.Any("err", err),
			)
			return
		}

		val, err = r.Client.SetNX(ctx, key, byteValue, expiration).Result()
		if err != nil && err != redis.Nil {
			zlog.Info("SetInterfaceNx err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("value", value))
		}
	})
	return val, err
}
func (r *RedisApi) Get(ctx context.Context, key string, timeout time.Duration) (val string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.Get(ctx, key).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("Get err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("err", err))
		}
	})
	return val, err
}

// GetAndUnmarshal 获取字符串并序列化
func (r *RedisApi) GetAndUnmarshal(ctx context.Context, key string, dst interface{}, timeout time.Duration) (err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		var val string
		val, err = r.Client.Get(ctx, key).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("Get err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("err", err),
			)
			return
		}
		if err == redis.Nil {
			return
		}

		err = json.Unmarshal([]byte(val), dst)
		if err != nil {
			zlog.Error("Get Unmarshal err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.String("val", val),
				zap.Any("dst", dst),
				zap.Any("err", err))
		}
	})
	return err
}

func (r *RedisApi) MGet(ctx context.Context, timeout time.Duration, keys []string) (val []interface{}, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.MGet(ctx, keys...).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("Get err",
				zap.String("ServiceName", r.ServiceName),
				zap.Any("key", keys),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) Del(ctx context.Context, timeout time.Duration, keys ...string) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.Del(ctx, keys...).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("Del err",
				zap.String("ServiceName", r.ServiceName),
				zap.Any("keys", keys),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) IncrBy(ctx context.Context, timeout time.Duration, key string, value int64) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.IncrBy(ctx, key, value).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("IncrBy err",
				zap.String("ServiceName", r.ServiceName),
				zap.Any("key", key),
				zap.Any("value", value),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) Expire(ctx context.Context, timeout time.Duration, key string, expiration time.Duration) (val bool, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.Expire(ctx, key, expiration).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("Expire err",
				zap.String("ServiceName", r.ServiceName),
				zap.Any("key", key),
				zap.Any("expiration", expiration),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ExpireAt(ctx context.Context, timeout time.Duration, key string, expiration time.Time) (val bool, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ExpireAt(ctx, key, expiration).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ExpireAt err",
				zap.String("ServiceName", r.ServiceName),
				zap.Any("key", key),
				zap.Any("expiration", expiration),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) HIncrBy(ctx context.Context, key string, field string, incr int64, timeout time.Duration) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.HIncrBy(ctx, key, field, incr).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("HIncrBy err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.String("field", field),
				zap.Int64("incr", incr),
				zap.Any("err", err))
		}
	})
	return val, err
}

// HGetAll 获取hash中数据，  尽量在 dao 层使用 HGetAllScan 将数据读取到结构体中增强可读性和可维护性
func (r *RedisApi) HGetAll(ctx context.Context, key string, timeout time.Duration) (val map[string]string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.HGetAll(ctx, key).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("HGetAll err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("err", err))
		}
	})
	return val, err
}

// HGetAllScan 将hash中数据读到结构体中
func (r *RedisApi) HGetAllScan(ctx context.Context, key string, dst interface{}, timeout time.Duration) (err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		err = r.Client.HGetAll(ctx, key).Scan(dst)
		if err != nil && err != redis.Nil {
			zlog.Error("HGetAllScan err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("dst", dst),
				zap.Any("err", err))
		}
	})
	return err
}

func (r *RedisApi) HGet(ctx context.Context, key string, field string, timeout time.Duration) (val string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.HGet(ctx, key, field).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("HGet err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.String("field", field),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) HSet(ctx context.Context, key string, timeout time.Duration, values ...interface{}) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.HSet(ctx, key, values...).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("HMSet err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("values", values),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) HMSet(ctx context.Context, key string, timeout time.Duration, values ...interface{}) (val bool, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.HMSet(ctx, key, values...).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("HMSet err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("values", values),
				zap.Any("err", err))
		}
	})
	return val, err
}

//func (r *RedisApi) HSetStruct(ctx context.Context, key string, values interface{}, timeout time.Duration) (val bool, err error) {
//	r.do(ctx, timeout, func(ctx context.Context) {
//		var hashData map[string]interface{}
//		hashData, err = tools.StructToMapByRedisTag(values)
//		if err != nil {
//			zlog.Error("HMSet StructToMapByRedisTag err",
//				zap.Any("values", values),
//				zap.Any("err", err))
//			return
//		}
//		val, err = r.Client.HMSet(ctx, key, hashData).Result()
//		if err != nil && err != redis.Nil {
//			zlog.Error("HMSet err",
//				zap.String("ServiceName", r.ServiceName),
//				zap.String("key", key),
//				zap.Any("values", values),
//				zap.Any("err", err))
//		}
//	})
//	return val, err
//}

func (r *RedisApi) HMGet(ctx context.Context, key string, timeout time.Duration, fields ...string) (val []interface{}, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.HMGet(ctx, key, fields...).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("HMGet err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("fields", fields),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) HMGetScan(ctx context.Context, key string, dst interface{}, timeout time.Duration, fields ...string) (err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		err = r.Client.HMGet(ctx, key, fields...).Scan(dst)
		if err != nil && err != redis.Nil {
			zlog.Error("HMGetScan err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("fields", fields),
				zap.Any("err", err))
		}
	})
	return err
}

func (r *RedisApi) HMGetStringValue(ctx context.Context, key string, timeout time.Duration, fields ...string) (val map[string]string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		data, err := r.Client.HMGet(ctx, key, fields...).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("HMGet err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("fields", fields),
				zap.Any("err", err))
		}
		val = make(map[string]string)
		for i := range data {
			val[fields[i]], _ = data[i].(string)
		}

	})
	return val, err
}

func (r *RedisApi) ZAdd(ctx context.Context, key string, timeout time.Duration, members ...*redis.Z) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZAdd(ctx, key, members...).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZAdd err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("members", members),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZRange(ctx context.Context, key string, start int64, stop int64, timeout time.Duration) (val []string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZRange(ctx, key, start, stop).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZRange err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Int64("start", start),
				zap.Int64("stop", stop),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZRevRange(ctx context.Context, key string, start int64, stop int64, timeout time.Duration) (val []string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZRevRange(ctx, key, start, stop).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZRange err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Int64("start", start),
				zap.Int64("stop", stop),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZScore(ctx context.Context, key, member string, timeout time.Duration) (val float64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZScore(ctx, key, member).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZScore err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.String("member", member),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZRem(ctx context.Context, key, member string, timeout time.Duration) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZRem(ctx, key, member).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZRem err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.String("member", member),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZCard(ctx context.Context, key string, timeout time.Duration) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZCard(ctx, key).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZCard err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("err", err))
		}
	})
	return val, err
}

// ZCount 获取 score >= min && score <= max 的总数
func (r *RedisApi) ZCount(ctx context.Context, key string, min string, max string, timeout time.Duration) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZCount(ctx, key, min, max).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZCard err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) SIsMember(ctx context.Context, key string, member string, timeout time.Duration) (val bool, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.SIsMember(ctx, key, member).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("SIsMember err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.String("member", member),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) SMembers(ctx context.Context, key string, timeout time.Duration) (val []string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.SMembers(ctx, key).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("SMembers err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key))
		}
	})
	return val, err
}

func (r *RedisApi) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64, timeout time.Duration) (val []redis.Z, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZRangeWithScores(ctx, key, start, stop).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZRangeWithScores err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Int64("start", start),
				zap.Int64("stop", stop),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZRevRangeWithScores(ctx context.Context, key string, start int64, stop int64, timeout time.Duration) (val []redis.Z, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZRevRangeWithScores(ctx, key, start, stop).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZRevRangeWithScores err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Int64("start", start),
				zap.Int64("stop", stop),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZRevRangeByScores(ctx context.Context, key string, zrangeby redis.ZRangeBy, timeout time.Duration) (val []string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZRevRangeByScore(ctx, key, &zrangeby).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZRevRangeByScores err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Any("zrangeby", zrangeby),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) ZInterStore(ctx context.Context, destination string, store *redis.ZStore, timeout time.Duration) (val int64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.ZInterStore(ctx, destination, store).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("ZInterStore err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("destination", destination),
				zap.Any("store", store),
				zap.Any("err", err))
		}
	})
	return val, err
}

func (r *RedisApi) LRange(ctx context.Context, key string, start int64, stop int64, timeout time.Duration) (val []string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, err = r.Client.LRange(ctx, key, start, stop).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("SMembers err",
				zap.String("ServiceName", r.ServiceName),
				zap.String("key", key),
				zap.Error(err))
		}
	})
	return val, err
}

// PipeHMGetKeys 批量获取hash中的key
func (r *RedisApi) PipeHMGetKeys(ctx context.Context, keys []string, fields []string, timeout time.Duration) (val map[string]map[string]string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val = make(map[string]map[string]string)
		cmdList := make([]*redis.SliceCmd, len(keys))

		pipe := r.Client.Pipeline()
		for index, key := range keys {
			cmdList[index] = pipe.HMGet(ctx, key, fields...)
		}
		_, err = pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			zlog.Error("PipeHMGet execErr",
				zap.String("ServiceName", r.ServiceName),
				zap.Any("keys", keys),
				zap.Any("fields", fields),
				zap.Error(err))
			return
		}

		for index, cmd := range cmdList {
			record, cmdErr := cmd.Result()
			if cmdErr != nil && cmdErr != redis.Nil {
				zlog.Error("PipeHMGet cmdErr",
					zap.String("ServiceName", r.ServiceName),
					zap.Any("cmd", cmd.String()),
					zap.Error(err))
			}

			key := keys[index]
			temp := make(map[string]string)
			for i, value := range record {
				field := fields[i]
				valueStr, _ := value.(string)
				temp[field] = valueStr
			}
			val[key] = temp
		}
	})
	return val, err
}

func (r *RedisApi) Scan(ctx context.Context, cursor uint64, match string, count int64, timeout time.Duration) (val []string, newCursor uint64, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val, newCursor, err = r.Client.Scan(ctx, cursor, match, count).Result()
		if err != nil && err != redis.Nil {
			zlog.Error("Scan err",
				zap.String("ServiceName", r.ServiceName),
				zap.Uint64("cursor", cursor),
				zap.String("match", match),
				zap.Int64("count", count),
				zap.Error(err))
		}
	})
	return val, newCursor, err
}

func (r *RedisApi) ScanIterator(ctx context.Context, cursor uint64, match string, count int64, timeout time.Duration) (iterator *redis.ScanIterator) {
	r.do(ctx, timeout, func(ctx context.Context) {
		iterator = r.Client.Scan(ctx, cursor, match, count).Iterator()
	})
	return iterator
}

func (r *RedisApi) PipeHMGetKeysByPrefix(ctx context.Context, prefix string, keys []string, fields []string, timeout time.Duration) (val map[string]map[string]string, err error) {
	var rdbKeys []string
	for i := range keys {
		rdbKeys = append(rdbKeys, prefix+keys[i])
	}
	cacheData, err := r.PipeHMGetKeys(ctx, rdbKeys, fields, timeout)
	if err != nil {
		return val, err
	}
	val = make(map[string]map[string]string)
	for i := range keys {
		val[keys[i]] = cacheData[prefix+keys[i]]
	}
	return val, nil
}

// PipeHGetAllKeys 批量获取hash中的key
func (r *RedisApi) PipeHGetAllKeys(ctx context.Context, keys []string, timeout time.Duration) (val map[string]map[string]string, err error) {
	r.do(ctx, timeout, func(ctx context.Context) {
		val = make(map[string]map[string]string)
		cmdList := make([]*redis.StringStringMapCmd, len(keys))

		pipe := r.Client.Pipeline()
		for index, key := range keys {
			cmdList[index] = pipe.HGetAll(ctx, key)
		}
		_, err = pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			zlog.Error("PipeHGetAll execErr",
				zap.String("ServiceName", r.ServiceName),
				zap.Any("keys", keys),
				zap.Error(err))
			return
		}

		for index, cmd := range cmdList {
			record, cmdErr := cmd.Result()
			if cmdErr != nil && cmdErr != redis.Nil {
				zlog.Error("PipeHGetAll cmdErr",
					zap.String("ServiceName", r.ServiceName),
					zap.Any("cmd", cmd.String()),
					zap.Error(err))
			}

			val[keys[index]] = record
		}
	})
	return val, err
}

func (r *RedisApi) PipeHGetAllKeysByPrefix(ctx context.Context, prefix string, keys []string, timeout time.Duration) (val map[string]map[string]string, err error) {
	var rdbKeys []string
	for i := range keys {
		rdbKeys = append(rdbKeys, prefix+keys[i])
	}
	cacheData, err := r.PipeHGetAllKeys(ctx, rdbKeys, timeout)
	if err != nil {
		return val, err
	}
	val = make(map[string]map[string]string)
	for i := range keys {
		val[keys[i]] = cacheData[prefix+keys[i]]
	}
	return val, nil
}

func (r *RedisApi) CheckExpireByPreRefresh(ctx context.Context, key string, factor int, timeout time.Duration) (isExpire bool, expireTime time.Duration) {
	r.do(ctx, timeout, func(ctx context.Context) {
		isExpire = true
		if factor <= 0 {
			factor = 2
		}
		var err error
		expireTime, err = r.Client.TTL(ctx, key).Result()
		if err != nil && err != redis.Nil {
			return
		}

		randInt := rand.Intn(100)
		if randInt == 0 {
			randInt = 1
		}
		if expireTime > 0*time.Second && expireTime.Seconds() > -float64(factor)*math.Log(float64(randInt)/float64(100)) {
			isExpire = false
		}
	})
	return isExpire, expireTime
}
