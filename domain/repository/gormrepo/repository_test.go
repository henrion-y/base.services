package gormrepo

import (
	"context"
	"github.com/henrion-y/base.services/database/gorm"
	"github.com/spf13/viper"
	"testing"
	"time"

	_gorm "gorm.io/gorm"

	"github.com/henrion-y/base.services/domain/repository"
)

type User struct {
	ID    int        `json:"id" gorm:"primary_key"`
	Name  string     `json:"name" gorm:"name"`
	Age   int        `json:"age" gorm:"age"`
	Ctime time.Time  `json:"ctime" gorm:"update_time_stamp"`
	Mtime time.Time  `json:"mtime" gorm:"update_time_stamp"`
	Dtime *time.Time `json:"dtime" gorm:"dtime"`
}

func (t *User) TableName() string {
	return "t_user_repository"
}

func getDB() *_gorm.DB {
	v := viper.New()
	v.Set("database.Driver", "mysql")
	v.Set("database.User", "root")
	v.Set("database.Password", "123456")
	v.Set("database.Host", "127.0.0.1:3306")
	v.Set("database.Db", "base_test")
	v.Set("database.Charset", "utf8")
	db, err := gorm.NewDbProvider(v)
	if err != nil {
		panic(err)
	}
	return db
}

func TestBaseRepository_Create(t *testing.T) {
	mysqlConn := getDB()

	err := mysqlConn.AutoMigrate(&User{})
	if err != nil {
		t.Fatal(err)
	}

	newTime := time.Now()
	user1 := &User{
		//ID:    3,
		Name:  "张飞",
		Age:   28,
		Ctime: newTime,
		Mtime: newTime,
	}

	user2 := &User{
		Name:  "关羽",
		Age:   21,
		Ctime: newTime,
		Mtime: newTime,
	}
	db := getDB()
	repo := NewBaseRepository(db)

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
	db := getDB()
	repo := NewBaseRepository(db)

	var list []User
	mod := User{}
	filterGroup := repository.NewFilterGroup().IsNotNull("dtime")

	err := repo.Find(context.Background(), &mod, &list, nil, filterGroup, nil, nil)

	if err != nil {
		t.Fatal(err)
	}
	t.Log(list)

	list2 := []User{}
	err = repo.Find(context.Background(), &mod, &list2, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(list2)
}

// 游标查询
func TestBaseRepository_Find2(t *testing.T) {
	db := getDB()
	repo := NewBaseRepository(db)

	mod := User{}
	var list []User
	filterGroup := repository.NewFilterGroup().Equals("name", "张飞").GreaterThanOrEqual("age", 10).
		And(
			repository.NewFilterGroup().GreaterThan("id", 0),
			repository.NewFilterGroup().In("age", []int{18, 19, 20, 21, 22, 23, 24, 25, 28}),
		)
	sortSpecs := repository.NewSortSpecs("age", repository.SortType_DESC).Add("id", repository.SortType_ASC)
	limitSpec := repository.NewLimitSpec(0, 20)

	err := repo.Find(context.Background(), &mod, &list, []string{"name", "age"}, filterGroup, sortSpecs, limitSpec)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(list)
}

func TestFindOne(t *testing.T) {
	db := getDB()
	repo := NewBaseRepository(db)

	mod := User{}
	err := repo.FindOne(context.Background(), &mod, []string{"name", "age"}, repository.NewFilterGroup().Equals("name", "张飞"),
		repository.NewSortSpecs("age", repository.SortType_DESC))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(mod)
}

func TestBaseRepository_Count(t *testing.T) {
	db := getDB()
	repo := NewBaseRepository(db)

	mod := User{}
	count, err := repo.Count(context.Background(), &mod, repository.NewFilterGroup().Equals("name", "张飞"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log(count)
}

func TestBaseRepository_Update(t *testing.T) {
	db := getDB()
	repo := NewBaseRepository(db)

	mod := User{}

	data := map[string]interface{}{
		"age": 19,
	}

	rowCount, err := repo.Update(context.Background(), &mod, data, repository.NewFilterGroup().AddFilter("name", repository.FilterType_EQ, "张飞"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rowCount)
}

func TestBaseRepository_Delete(t *testing.T) {
	db := getDB()
	repo := NewBaseRepository(db)

	mod := User{}

	err := repo.Delete(context.Background(), &mod, repository.NewFilterGroup().AddFilter("id", repository.FilterType_EQ, 3))
	if err != nil {
		t.Fatal(err)
	}
}
