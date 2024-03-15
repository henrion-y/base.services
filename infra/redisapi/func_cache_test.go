package redisapi

import (
	"context"
	"testing"
	"time"
)

func myFunction(x int, y int) int {
	return x + y
}

type TData struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func myFuncStruct(t1 TData, t2 *TData) (TData, error) {

	return TData{
		Name: t1.Name,
		Age:  t2.Age,
	}, nil
}

func myFuncMap(m1 map[string]string, m2 map[string]string) (map[string]string, error) {
	for k, v := range m1 {
		m2[k] = v
	}
	return m2, nil
}

func myFunc0Params() string {
	return "hello"
}

func TestCacheFunction(t *testing.T) {
	var data int
	redisApi := getDb()
	err := redisApi.GetFuncResultByCache(context.Background(), myFunction, "", 0, 60*time.Second, &data, 1, 2)
	if err != nil {
		return
	}
	t.Log(data)

	t1 := TData{Name: "134542332"}
	t2 := TData{Age: 3431}
	t3 := TData{}
	err = redisApi.GetFuncResultByCache(context.Background(), myFuncStruct, "", 0, 60*time.Second, &t3, t1, &t2)
	if err != nil {
		return
	}
	t.Log(t3)

	m1 := map[string]string{
		"name": "132131",
		"coin": "btc",
	}

	m2 := map[string]string{
		"show": "coin",
		"logo": "23f34",
	}
	m3 := make(map[string]string)

	err = redisApi.GetFuncResultByCache(context.Background(), myFuncMap, "", 0, 60*time.Second, &m3, m1, m2)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Log(m3)

}
