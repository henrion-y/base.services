package mongorepo

import (
	"context"
	"github.com/henrion-y/base.services/domain/repository"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
	"time"
)

type User struct {
	ID    int        `json:"id" gorm:"primary_key" bson:"id"`
	Name  string     `json:"name" gorm:"name" bson:"name"`
	Age   int        `json:"age" gorm:"age" bson:"age"`
	Ctime time.Time  `json:"ctime" gorm:"update_time_stamp" bson:"ctime"`
	Mtime time.Time  `json:"mtime" gorm:"update_time_stamp" bson:"mtime"`
	Dtime *time.Time `json:"dtime" gorm:"dtime" bson:"dtime"`
}

func getDb() *mongo.Database {
	// todo 数据库初始化
	panic("数据库初始化")
}

func (t User) TableName() string {
	return "t_user_repository"
}

func TestCreate(t *testing.T) {
	newTime := time.Now()
	user1 := &User{
		//ID:    3,
		Name:  "张三",
		Age:   28,
		Ctime: newTime,
		Mtime: newTime,
	}

	user2 := &User{
		Name:  "关二",
		Age:   21,
		Ctime: newTime,
		Mtime: newTime,
	}

	repo := NewBaseRepository(getDb())

	{
		err := repo.Create(context.Background(), user1)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second * 3)

		err = repo.Create(context.Background(), user2)
		if err != nil {
			t.Fatal(err)
		}

		t.Log("插入记录成功")
	}
}

func TestBaseRepository_Find(t *testing.T) {
	repo := NewBaseRepository(getDb())

	var list []User
	mod := User{}
	filterGroup := repository.NewFilterGroup().Equals("name", "张飞").GreaterThanOrEqual("age", 10).
		And(repository.NewFilterGroup().GreaterThan("id", 1), repository.NewFilterGroup().In("age", []int{18, 19, 20, 21, 22, 23, 24, 25}))

	err := repo.Find(context.Background(), &mod, &list, nil, filterGroup, nil, nil)

	if err != nil {
		t.Fatal(err)
	}
	t.Log(list)

	var list2 []User
	err = repo.Find(context.Background(), &mod, &list2, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(list2)
}

// 游标查询
func TestBaseRepository_Find2(t *testing.T) {
	repo := NewBaseRepository(getDb())

	group := repository.NewFilterGroup().GreaterThan("age", 10)

	var list []User
	mod := User{}
	err := repo.Find(context.Background(), &mod, &list, []string{"name", "age"}, group,
		repository.NewSortSpecs("age", repository.SortType_DESC).Add("id", repository.SortType_ASC), repository.NewLimitSpec(0, 20))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(list)
}

func TestFindOne(t *testing.T) {
	repo := NewBaseRepository(getDb())

	mod := User{}
	err := repo.FindOne(context.Background(), &mod, []string{"name", "age"}, repository.NewFilterGroup(),
		repository.NewSortSpecs("age", repository.SortType_DESC))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(mod)
}

func TestBaseRepository_Count(t *testing.T) {
	repo := NewBaseRepository(getDb())

	mod := User{}

	count, err := repo.Count(context.Background(), &mod, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(count)
}

func TestBaseRepository_Update(t *testing.T) {
	repo := NewBaseRepository(getDb())

	mod := User{}

	data := map[string]interface{}{
		"age": 19,
	}

	err := repo.Update(context.Background(), &mod, data, repository.NewFilterGroup().AddFilter("name", repository.FilterType_EQ, "张飞"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestBaseRepository_Delete(t *testing.T) {
	repo := NewBaseRepository(getDb())

	mod := User{}

	err := repo.Delete(context.Background(), &mod, repository.NewFilterGroup().AddFilter("id", repository.FilterType_EQ, 3))
	if err != nil {
		t.Fatal(err)
	}
}
