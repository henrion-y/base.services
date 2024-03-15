package redisapi

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/henrion-y/base.services/infra/zlog"
	"go.uber.org/zap"
	"path/filepath"
	"reflect"
	"runtime"
	"time"
)

// GetFuncCacheKeyByArgs 通过函数名称侧参数生成缓存key
func (r *RedisApi) GetFuncCacheKeyByArgs(function interface{}, args ...interface{}) string {
	argsHash := ""
	if len(args) > 0 {
		// #nosec G401
		hasher := sha1.New()

		for _, arg := range args {
			reflectArg := reflect.ValueOf(arg)
			for reflectArg.Kind() == reflect.Ptr {
				if reflectArg.IsNil() {
					hasher.Write([]byte("nilPtr"))
					break
				}
				reflectArg = reflectArg.Elem()
			}

			if reflectArg.IsValid() && reflectArg.CanInterface() {
				typeInfo := reflect.TypeOf(reflectArg.Interface()).String()
				hasher.Write([]byte(typeInfo))
				hasher.Write([]byte{0})
				hasher.Write([]byte(fmt.Sprintf("%v", reflectArg.Interface())))
			} else {
				hasher.Write([]byte("invalidOrNonInterfacedValue"))
			}

			hasher.Write([]byte{0xff})
		}

		hashBytes := hasher.Sum(nil)
		hashString := hex.EncodeToString(hashBytes)
		argsHash = hashString[:6]
	}

	funcName := runtime.FuncForPC(reflect.ValueOf(function).Pointer()).Name()
	funcName = filepath.Ext(funcName)[1:]

	return "cacheFunc:" + funcName + ":" + argsHash
}

// GetFuncResultByCache 通过缓存获取函数执行结果
func (r *RedisApi) GetFuncResultByCache(ctx context.Context, function interface{}, cacheKey string, cacheResultIndex int, expire time.Duration, result interface{}, args ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			zlog.Error("GetFuncResultByCache", zap.Any("recover", r))
			err = fmt.Errorf("%v", r)
		}
	}()

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.IsNil() {
		return fmt.Errorf("result must be a non-nil pointer")
	}

	if cacheKey == "" {
		cacheKey = r.GetFuncCacheKeyByArgs(function, args...)
	}

	err = r.GetAndUnmarshal(ctx, cacheKey, result, 0)
	if err == nil {
		return nil
	}

	return r.doFuncSetResult2Cache(ctx, function, cacheKey, cacheResultIndex, expire, resultVal, args...)
}

// GetFuncResultByDefaultCacheKey 通过默认缓存key获取函数执行结果
func (r *RedisApi) GetFuncResultByDefaultCacheKey(ctx context.Context, function interface{}, cacheResultIndex int, expire time.Duration, result interface{}, args ...interface{}) (err error) {
	return r.GetFuncResultByCache(ctx, function, "", cacheResultIndex, expire, result, args...)
}

// GetFuncResultByPreRefreshCache 通过预刷新策略缓存获取函数执行结果
func (r *RedisApi) GetFuncResultByPreRefreshCache(ctx context.Context, function interface{}, cacheKey string, cacheResultIndex int, expire time.Duration, factor int, result interface{}, args ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			zlog.Error("GetFuncResultByPreRefreshCache", zap.Any("recover", r))
			err = fmt.Errorf("%v", r)
		}
	}()

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.IsNil() {
		return fmt.Errorf("result must be a non-nil pointer")
	}

	if cacheKey == "" {
		cacheKey = r.GetFuncCacheKeyByArgs(function, args...)
	}

	if isExpire, _ := r.CheckExpireByPreRefresh(ctx, cacheKey, factor, 0); !isExpire {
		err := r.GetAndUnmarshal(ctx, cacheKey, result, 0)
		if err == nil {
			return nil
		}
	}

	return r.doFuncSetResult2Cache(ctx, function, cacheKey, cacheResultIndex, expire, resultVal, args...)
}

func (r *RedisApi) GetFuncResultBySyncPreRefreshCache(ctx context.Context, function interface{}, cacheKey string, cacheResultIndex int, expire time.Duration, factor int, result interface{}, args ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			zlog.Error("GetFuncResultByPreRefreshCache", zap.Any("recover", r))
			err = fmt.Errorf("%v", r)
		}
	}()

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.IsNil() {
		return fmt.Errorf("result must be a non-nil pointer")
	}

	if cacheKey == "" {
		cacheKey = r.GetFuncCacheKeyByArgs(function, args...)
	}

	if isExpire, expireTime := r.CheckExpireByPreRefresh(ctx, cacheKey, factor, 0); !isExpire {
		err := r.GetAndUnmarshal(ctx, cacheKey, result, 0)
		if err == nil {
			return nil
		}
	} else if expireTime > 0 {
		err := r.GetAndUnmarshal(ctx, cacheKey, result, 0)
		if err == nil {
			go func() {
				_ = r.doFuncSetResult2Cache(ctx, function, cacheKey, cacheResultIndex, expire, resultVal, args...)
			}()
			return nil
		}
	}

	return r.doFuncSetResult2Cache(ctx, function, cacheKey, cacheResultIndex, expire, resultVal, args...)
}

func (r *RedisApi) doFuncSetResult2Cache(ctx context.Context, function interface{}, cacheKey string, cacheResultIndex int, expire time.Duration, resultVal reflect.Value, args ...interface{}) error {
	funcValue := reflect.ValueOf(function)
	in := make([]reflect.Value, len(args))
	for i := range args {
		in[i] = reflect.ValueOf(args[i])
	}
	functionResults := funcValue.Call(in)

	resultValue := functionResults[cacheResultIndex]

	if resultValue.Kind() == reflect.Invalid {
		return fmt.Errorf("result is invalid")
	}

	resultElem := resultVal.Elem()
	if resultElem.Kind() != resultValue.Kind() {
		return fmt.Errorf("result type is not compatible with function result")
	}
	resultElem.Set(resultValue)

	_, err := r.SetInterface(ctx, cacheKey, resultVal.Interface(), expire, 0)
	if err != nil {
		return err
	}
	return nil
}
