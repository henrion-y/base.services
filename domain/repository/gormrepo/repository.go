package gormrepo

import (
	"context"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/henrion-y/base.services/domain/repository"
	"github.com/henrion-y/base.services/infra/zlog"
)

type gormRepository struct {
	Db *gorm.DB
}

func NewBaseRepository(db *gorm.DB) repository.BaseRepository {
	return &gormRepository{Db: db}
}

func (r *gormRepository) Create(ctx context.Context, mod repository.Model) error {
	err := r.Db.Create(mod).Error
	if err != nil {
		zlog.Error("gormRepo.Create", zap.Any("mod", mod), zap.Error(err))
	}
	return err
}

func (r *gormRepository) Update(ctx context.Context, mod repository.Model, data map[string]interface{}, filterGroup *repository.FilterGroup) error {
	mysqlConn := r.Db.Table(mod.TableName())

	if filterGroup != nil {
		mysqlConn = filterGroup.BuildToMysql(mysqlConn)
	}

	err := mysqlConn.Updates(data).Error
	if err != nil {
		zlog.Error("gormRepo.Update", zap.Any("mod", mod), zap.Any("data", data), zap.Any("filterGroup", filterGroup), zap.Error(err))
	}
	return err
}

func (r *gormRepository) Delete(ctx context.Context, mod repository.Model, filterGroup *repository.FilterGroup) error {
	mysqlConn := r.Db.Table(mod.TableName())

	if filterGroup != nil {
		mysqlConn = filterGroup.BuildToMysql(mysqlConn)
	}

	err := mysqlConn.Delete(mod).Error
	if err != nil {
		zlog.Error("gormRepo.Delete", zap.Any("mod", mod), zap.Any("filterGroup", filterGroup), zap.Error(err))
	}
	return err
}

func (r *gormRepository) Find(ctx context.Context, mod repository.Model, result interface{}, fields []string, filterGroup *repository.FilterGroup, sortSpecs *repository.SortSpecs, limitSpec *repository.LimitSpec) error {
	mysqlConn := r.Db.Table(mod.TableName())

	if len(fields) > 0 {
		mysqlConn = mysqlConn.Select(fields)
	}

	if filterGroup != nil {
		mysqlConn = filterGroup.BuildToMysql(mysqlConn)
	}
	if sortSpecs != nil {
		sortSpecs.BuildToMysql(mysqlConn)
	}
	if limitSpec != nil {
		limitSpec.BuildToMysql(mysqlConn)
	}

	err := mysqlConn.Scan(result).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		zlog.Error("gormRepo.Find", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Any("sortSpecs", sortSpecs),
			zap.Any("limitSpec", limitSpec),
			zap.Error(err))
		return err
	}
	return nil
}

func (r *gormRepository) FindOne(ctx context.Context, mod repository.Model, fields []string, filterGroup *repository.FilterGroup, sortSpecs *repository.SortSpecs) error {
	mysqlConn := r.Db.Table(mod.TableName())

	if len(fields) > 0 {
		mysqlConn = mysqlConn.Select(fields)
	}

	mysqlConn = filterGroup.BuildToMysql(mysqlConn)
	if filterGroup != nil {
		mysqlConn = filterGroup.BuildToMysql(mysqlConn)
	}
	if sortSpecs != nil {
		sortSpecs.BuildToMysql(mysqlConn)
	}
	limitSpec := repository.NewLimitSpec(0, 1)
	limitSpec.BuildToMysql(mysqlConn)

	err := mysqlConn.Scan(mod).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		zlog.Error("gormRepo.FindOne", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Any("sortSpecs", sortSpecs),
			zap.Any("limitSpec", limitSpec),
			zap.Error(err))
		return err
	}
	return nil
}

func (r *gormRepository) Count(ctx context.Context, mod repository.Model, filterGroup *repository.FilterGroup) (int64, error) {
	mysqlConn := r.Db.Table(mod.TableName())

	var count int64
	if filterGroup != nil {
		mysqlConn = filterGroup.BuildToMysql(mysqlConn)
	}

	err := mysqlConn.Count(&count).Error
	if err != nil {
		zlog.Error("gormRepo.Count", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Error(err))
		return count, err
	}
	return count, nil
}
