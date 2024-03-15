package redisapi

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var dataLuck = map[string]int{
	"test": 1,
}

var dataNotLuck = map[string]int{
	"test": 1,
}

func TestGenerateRandomString(t *testing.T) {
	go func() {
		g1 := generateRandomString(10)
		t.Log(g1)
	}()
	go func() {
		g2 := generateRandomString(10)
		t.Log(g2)
	}()
	time.Sleep(2 * time.Second)
}

func task() {
	key := "test:redLock"
	redLock, err := NewRedLock(getDb(), 10*time.Second)
	if err != nil {
		fmt.Println(err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)

	err = redLock.GrabLock(ctx, key)
	if err != nil {
		cancelFunc()
		fmt.Println(err)
	}
	dataLuck["test"]++
	fmt.Println(dataLuck)
	time.Sleep(3 * time.Second)
	err = redLock.ReleaseLock(cancelFunc, key)
	if err != nil {
		fmt.Println(err)
	}
}

func testNotLock() {
	dataNotLuck["test"]++
	fmt.Println(dataNotLuck)
}

func TestRedLock(t *testing.T) {

	for i := 0; i < 20; i++ {
		go task()
		go testNotLock()
	}
	time.Sleep(20 * time.Second)
}
