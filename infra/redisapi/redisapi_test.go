package redisapi

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"strconv"
	"strings"
	"testing"
	"time"
)

func getDb() *RedisApi {
	v := viper.New()
	v.Set("redis.ServiceName", "main")
	v.Set("redis.Hosts", "127.0.0.1:6379")
	v.Set("redis.Password", "Zi5JgFOngJyymf2i")
	redisApi, err := NewRedisApiProvider(v)
	if err != nil {
		panic(err)
	}
	return redisApi
}

func TestSet(t *testing.T) {

	set2, err := getDb().Set(context.Background(), "testeerewfeqcvebw", "fwedwwq1ed", 1000*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(set2)
}

func TestGet(t *testing.T) {
	data, err := getDb().Get(context.Background(), "testeerewfeqcvebw", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(data)
}

func TestDel(t *testing.T) {
	data, err := getDb().Del(context.Background(), 0, "testeerewfeqcvebw")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(data)
}

func TestSetNx(t *testing.T) {
	nx, err := getDb().SetNx(context.Background(), "testsetNX22", "oubgh", 100*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(nx)
}
func TestPipeHMGet(t *testing.T) {
	val, _ := getDb().PipeHMGetKeys(context.Background(), []string{"index:coin:futures:bitcoin", "index:coin:futures:ethereum"}, []string{"volBuy24H", "volSell24H", "tra24HCny", "tra24HDeg24HCny"}, 0)
	t.Log(val)
}

func TestZInterStore(t *testing.T) {
	val, _ := getDb().ZInterStore(context.Background(), "testNewZset", &redis.ZStore{
		Keys:      []string{"zset1", "zset2"},
		Weights:   []float64{0, 1},
		Aggregate: "",
	}, 0)
	t.Log(val)
}

//func TestHMSet(t *testing.T) {
//	type Person struct {
//		Name    string `json:"name" bson:"name"`
//		Age     int    `json:"age" redis:"age"`
//		Address string `json:"address" redis:"address"`
//	}
//	person := Person{
//		Name:    "John Doe",
//		Age:     30,
//		Address: "123 Main St",
//	}
//	_, err := getDb().HSetStruct(context.Background(), "test:hmset:struct", person, 0)
//	if err != nil {
//		t.Fatal(err)
//	}
//}

func TestScan(t *testing.T) {
	cursor := uint64(0)
	for {
		keys, newCursor, err := getDb().Scan(context.Background(), cursor, "C_deg5MinUsd_up*", 100, 0)
		if err != nil {
			break
		}
		var delKeys []string
		for i := range keys {
			parses := strings.Split(keys[i], ":")
			if len(parses) > 1 {
				timeStr := parses[1]
				onlineTime, _ := strconv.ParseInt(timeStr, 10, 64)
				if onlineTime > time.Now().Unix()-60 {
					continue
				}
			}
			delKeys = append(delKeys, keys[i])
		}
		if len(delKeys) > 0 {
			_, _ = getDb().Del(context.Background(), 0, delKeys...)
		}
		cursor = newCursor
		if cursor == 0 {
			break
		}
	}
}

func TestScanIterator(t *testing.T) {
	iter := getDb().ScanIterator(context.Background(), 0, "C_deg5MinUsd_up*", 1, 0)
	var vals []string
	for iter.Next(context.Background()) {
		vals = append(vals, iter.Val())
	}
	t.Log(vals)
}

func TestHGetAllScan(t *testing.T) {
	type AppCoin struct {
		Timestamp    int64  `json:"timestamp" redis:"timestamp"`
		CoinType     string `json:"coin_type" redis:"coin_type"`
		UniqueKey    string `json:"unique_key" redis:"unique_key"`
		PublishPrice string `json:"publish_price" redis:"publish_price"`
	}
	appCoin := &AppCoin{}
	err := getDb().HGetAllScan(context.Background(), "app:bitcoin", appCoin, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*appCoin)

	data, _ := getDb().HGetAll(context.Background(), "app:bitcoin", 0)
	t.Log(data)
}

func TestHMGetScan(t *testing.T) {
	type AppCoin struct {
		Timestamp    int64  `json:"timestamp" redis:"timestamp"`
		CoinType     string `json:"coin_type" redis:"coin_type"`
		UniqueKey    string `json:"unique_key" redis:"unique_key"`
		PublishPrice string `json:"publish_price" redis:"publish_price"`
	}
	appCoin := &AppCoin{}
	err := getDb().HMGetScan(context.Background(), "app:bitcoin", appCoin, 0, "coin_type", "unique_key")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*appCoin)
}

func TestGetScan(t *testing.T) {
	type AppCoin struct {
		Timestamp    int64  `json:"timestamp" redis:"timestamp"`
		CoinType     string `json:"coin_type" redis:"coin_type"`
		UniqueKey    string `json:"unique_key" redis:"unique_key"`
		PublishPrice string `json:"publish_price" redis:"publish_price"`
	}
	appCoin := &AppCoin{}
	err := getDb().GetAndUnmarshal(context.Background(), "app:bitcoin:str", appCoin, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(appCoin)

	_, err = getDb().SetInterface(context.Background(), "app:bitcoin:str", *appCoin, 0, 0)
	if err != nil {
		return
	}
}

func TestGetScan2(t *testing.T) {
	result := make(map[string]interface{})

	err := getDb().GetAndUnmarshal(context.Background(), "app:bitcoin:str", &result, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}
