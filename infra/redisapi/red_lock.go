package redisapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

type RedLock struct {
	uuid       string
	expiration time.Duration
	RedisApi   *RedisApi
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

func generateRandomString(length int) string {
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// NewRedLock 创建red锁， expiration为锁默认过期时间
func NewRedLock(redisApi *RedisApi, expiration time.Duration) (*RedLock, error) {
	uuid := generateRandomString(10)
	return &RedLock{uuid: uuid, RedisApi: redisApi, expiration: expiration}, nil
}

// GrabLock 抢锁
func (s *RedLock) GrabLock(ctx context.Context, key string) error {
	retryCount := 5
EXIT:
	for {
		select {
		case <-ctx.Done():
			return errors.New("抢锁超时")
		default:
			// 通过 SetNx 查看key是否已存在，
			ok, _ := s.RedisApi.SetNx(ctx, key, s.uuid, s.expiration, 0)
			if !ok {
				// 如果存在则说明锁正在被他人持有，本轮抢锁失败
				if retryCount < 0 {
					//判断是否超高自旋次数，是则休眠后抢锁(这里可能需要引入释放锁通知机制主动唤醒)
					time.Sleep(5 * time.Microsecond)
				} else {
					// 没超过自旋次数则消耗自旋次数并直接重新抢锁
					retryCount--
				}
				continue
			}
			// 如果不存在则设置key和过期时间，代表抢锁成功, 跳出循环
			break EXIT
		}
	}
	fmt.Println("抢锁成功 ： key ,", key, " uuid ", s.uuid)
	go s.extendLock(ctx, key, int64(s.expiration))
	return nil
}

// 续签锁
func (s *RedLock) extendLock(ctx context.Context, key string, expire int64) {
	t := time.Duration(math.Ceil(float64(expire) / 2))
	times := 0
	for times < 3 {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(t * time.Second)
			_, err := s.RedisApi.Expire(ctx, 0, key, t*time.Second)
			if err != nil {
				fmt.Println("Expire err ", err)
				return
			}
			times++
		}
	}
}

// ReleaseLock 释放锁
func (s *RedLock) ReleaseLock(cancelFunc context.CancelFunc, key string) error {

	data, err := s.RedisApi.Get(context.Background(), key, 0)
	if err != nil {
		return err
	}
	if data != s.uuid {
		return errors.New("ReleaseLock other Lock")
	}
	_, err = s.RedisApi.Del(context.Background(), 0, key)
	if err != nil {
		fmt.Println("ReleaseLock err : ", err)
	}
	cancelFunc()
	return nil
}
