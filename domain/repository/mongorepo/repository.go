package mongorepo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"github.com/henrion-y/base.services/domain/repository"
	"github.com/henrion-y/base.services/infra/zlog"
)

type mongoRepository struct {
	Db *mongo.Database
}

func NewBaseRepository(db *mongo.Database) repository.BaseRepository {
	return &mongoRepository{Db: db}
}

func (r *mongoRepository) Create(ctx context.Context, mod repository.Model) error {
	collection := r.Db.Collection(mod.TableName())

	_, err := collection.InsertOne(ctx, mod)
	if err != nil {
		zlog.Error("mongoRepo.Create", zap.Any("mod", mod), zap.Error(err))
	}
	return err
}

func (r *mongoRepository) Update(ctx context.Context, mod repository.Model, data map[string]interface{}, filterGroup *repository.FilterGroup) error {
	collection := r.Db.Collection(mod.TableName())

	update := bson.D{}
	for k, v := range data {
		update = append(update, bson.E{Key: "$set", Value: bson.M{k: v}})
	}

	filter := bson.D{}
	if filterGroup != nil {
		filter = filterGroup.BuildToMongo()
	}

	_, err := collection.UpdateMany(ctx, filter, update)
	if err != nil {
		zlog.Error("mongoRepo.Update", zap.Any("mod", mod),
			zap.Any("data", data),
			zap.Any("filterGroup", filterGroup),
			zap.Error(err))
	}
	return err
}

func (r *mongoRepository) Delete(ctx context.Context, mod repository.Model, filterGroup *repository.FilterGroup) error {
	collection := r.Db.Collection(mod.TableName())

	filter := bson.D{}
	if filterGroup != nil {
		filter = filterGroup.BuildToMongo()
	}

	_, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		zlog.Error("mongoRepo.Delete", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Error(err))
	}
	return err
}

func (r *mongoRepository) Find(ctx context.Context, mod repository.Model, result interface{}, fields []string, filterGroup *repository.FilterGroup, sortSpecs *repository.SortSpecs, limitSpec *repository.LimitSpec) error {
	collection := r.Db.Collection(mod.TableName())

	filter := bson.D{}
	var sort interface{}
	var limit, skip *int64
	var formatProjection interface{}
	if len(fields) > 0 {
		projection := make(bson.D, len(fields))
		for index, field := range fields {
			projection[index] = bson.E{Key: field, Value: 1}
		}
		formatProjection = projection
	} else {
		formatProjection = nil
	}

	if filterGroup != nil {
		filter = filterGroup.BuildToMongo()
	}
	if sortSpecs != nil {
		sort = sortSpecs.BuildToMongo()
	}
	if limitSpec != nil {
		limit, skip = limitSpec.BuildToMongo()
	}

	option := options.FindOptions{
		Projection: formatProjection,
		Limit:      limit,
		Skip:       skip,
		Sort:       sort,
	}
	resultList, err := collection.Find(ctx, filter, &option)
	if err != nil && err != mongo.ErrNoDocuments {
		zlog.Error("mongoRepo.Find", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Any("sortSpecs", sortSpecs),
			zap.Any("limitSpec", limitSpec),
			zap.Any("fields", fields),
			zap.Error(err))
		return err
	}

	err = resultList.All(ctx, result)
	if err != nil && err != mongo.ErrNoDocuments {
		zlog.Error("mongoRepo.Find", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Any("sortSpecs", sortSpecs),
			zap.Any("limitSpec", limitSpec),
			zap.Any("fields", fields),
			zap.Error(err))
		return err
	}

	return nil
}

func (r *mongoRepository) FindOne(ctx context.Context, mod repository.Model, fields []string, filterGroup *repository.FilterGroup, sortSpecs *repository.SortSpecs) error {
	collection := r.Db.Collection(mod.TableName())

	filter := bson.D{}
	var sort interface{}
	var formatProjection interface{}
	if len(fields) > 0 {
		projection := make(bson.D, len(fields))
		for index, field := range fields {
			projection[index] = bson.E{Key: field, Value: 1}
		}
		formatProjection = projection
	} else {
		formatProjection = nil
	}
	if filterGroup != nil {
		filter = filterGroup.BuildToMongo()
	}
	if sortSpecs != nil {
		sort = sortSpecs.BuildToMongo()
	}

	option := options.FindOneOptions{
		Projection: formatProjection,
		Sort:       sort,
	}
	err := collection.FindOne(ctx, filter, &option).Decode(mod)
	if err != nil && err != mongo.ErrNoDocuments {
		zlog.Error("mongoRepo.FindOne", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Any("sortSpecs", sortSpecs),
			zap.Any("fields", fields),
			zap.Error(err))
		return err
	}

	return nil
}

func (r *mongoRepository) Count(ctx context.Context, mod repository.Model, filterGroup *repository.FilterGroup) (int64, error) {
	collection := r.Db.Collection(mod.TableName())

	filter := bson.D{}
	if filterGroup != nil {
		filter = filterGroup.BuildToMongo()
	}

	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		zlog.Error("mongoRepo.Count", zap.Any("mod", mod),
			zap.Any("filterGroup", filterGroup),
			zap.Error(err))
	}
	return count, err
}
