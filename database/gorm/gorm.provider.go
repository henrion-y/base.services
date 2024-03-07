package gorm

import (
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDbProvider(config *viper.Viper) (*gorm.DB, error) {
	driver := config.GetString("database.Driver")
	if len(driver) == 0 {
		return nil, errors.New("driver is empty")
	}

	user := config.GetString("database.User")
	if len(user) == 0 {
		return nil, errors.New("user is empty")
	}

	password := config.GetString("database.Password")
	if len(password) == 0 {
		return nil, errors.New("password is empty")
	}

	host := config.GetString("database.Host")
	if len(host) == 0 {
		return nil, errors.New("host is empty")
	}

	db := config.GetString("database.Db")
	if len(db) == 0 {
		return nil, errors.New("db is empty")
	}

	charset := config.GetString("database.Charset")
	if len(charset) == 0 {
		return nil, errors.New("charset is empty")
	}

	dial := "%s:%s@(%s)/%s?charset=%s&parseTime=True&loc=Local"
	dial = fmt.Sprintf(dial,
		user,
		password,
		host,
		db,
		charset)

	gormConf := &gorm.Config{}

	ormDb, err := gorm.Open(mysql.Open(dial), gormConf)
	if err != nil {
		return nil, err
	}

	return ormDb, nil
}
