package mongodb

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-funcards/slice"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	ErrMsgDecode  = "failed to decode document due to error: %w"
	ErrMsgQuery   = "failed to execute query due to error: %w"
	ErrMsgFromHex = "failed to convert hex to primitive.ObjectID due to error: %w"
)

var ErrNormalizeFilter = errors.New("couldn't normalize filter")

type Collection[T any] struct {
	Inner *mongo.Collection
	Log   *zap.Logger
}

func (c *Collection[T]) UseSession(ctx context.Context, fn func(mongo.SessionContext) error) error {
	return UseSession(ctx, c.Inner.Database().Client(), fn)
}

func (c *Collection[T]) InsertOne(ctx context.Context, document T, opts ...*options.InsertOneOptions) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.Log.Debug("document insert")
	result, err := c.Inner.InsertOne(ctx, document, opts...)
	if err != nil {
		return "", fmt.Errorf(ErrMsgQuery, err)
	}
	c.Log.Debug("document inserted", zap.Any("result", result))

	if oid, ok := result.InsertedID.(primitive.ObjectID); ok {
		return oid.Hex(), nil
	}

	return result.InsertedID.(string), nil
}

func (c *Collection[T]) InsertMany(ctx context.Context, documents []T, opts ...*options.InsertManyOptions) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.Log.Debug("documents insert")
	result, err := c.Inner.InsertMany(ctx, slice.Map(documents, func(document T) any {
		return document
	}), opts...)
	if err != nil {
		return nil, fmt.Errorf(ErrMsgQuery, err)
	}

	c.Log.Debug("documents inserted", zap.Any("result", result))

	return slice.Map(result.InsertedIDs, func(id any) string {
		if oid, ok := id.(primitive.ObjectID); ok {
			return oid.Hex()
		}
		return id.(string)
	}), nil
}

func (c *Collection[T]) UpdateOne(ctx context.Context, filter any, update any, opts ...*options.UpdateOptions) (err error) {
	filter, err = c.NormalizeFilter(filter)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.Log.Debug("document update")
	result, err := c.Inner.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return fmt.Errorf(ErrMsgQuery, err)
	}
	if result.MatchedCount == 0 && result.ModifiedCount == 0 && result.UpsertedCount == 0 {
		return fmt.Errorf(ErrMsgQuery, mongo.ErrNoDocuments)
	}
	c.Log.Debug("document updated", zap.Any("result", result))

	return nil
}

func (c *Collection[T]) DeleteOne(ctx context.Context, filter any, opts ...*options.DeleteOptions) (err error) {
	filter, err = c.NormalizeFilter(filter)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.Log.Debug("document delete")
	result, err := c.Inner.DeleteOne(ctx, filter, opts...)
	if err != nil {
		return fmt.Errorf(ErrMsgQuery, err)
	}
	if result.DeletedCount == 0 {
		return fmt.Errorf(ErrMsgQuery, mongo.ErrNoDocuments)
	}
	c.Log.Debug("document deleted", zap.Any("result", result))

	return nil
}

func (c *Collection[T]) FindOne(ctx context.Context, filter any, opts ...*options.FindOneOptions) (doc T, err error) {
	filter, err = c.NormalizeFilter(filter)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result := c.Inner.FindOne(ctx, filter, opts...)

	return c.Decode(result)
}

func (c *Collection[T]) Find(ctx context.Context, filter any, opts ...*options.FindOptions) (docs []T, err error) {
	filter, err = c.NormalizeFilter(filter)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cur, err := c.Inner.Find(ctx, filter, opts...)
	if err != nil {
		return docs, fmt.Errorf(ErrMsgQuery, err)
	}

	return c.All(ctx, cur)
}

func (c *Collection[T]) CountDocuments(ctx context.Context, filter any, opts ...*options.CountOptions) (count uint64, err error) {
	filter, err = c.NormalizeFilter(filter)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var total int64
	total, err = c.Inner.CountDocuments(ctx, filter, opts...)
	if err != nil {
		return 0, fmt.Errorf(ErrMsgQuery, err)
	}

	return uint64(total), nil
}

func (*Collection[T]) FindOptions(index uint64, size uint32) *options.FindOptions {
	return options.Find().SetSkip(int64(index)).SetLimit(int64(size))
}

func (*Collection[T]) Decode(r *mongo.SingleResult) (doc T, err error) {
	if r.Err() != nil {
		return doc, fmt.Errorf(ErrMsgQuery, r.Err())
	}
	if err = r.Decode(&doc); err != nil {
		return doc, fmt.Errorf(ErrMsgDecode, err)
	}
	return doc, nil
}

func (*Collection[T]) All(ctx context.Context, cur *mongo.Cursor) (docs []T, err error) {
	if cur.Err() != nil {
		return docs, fmt.Errorf(ErrMsgQuery, cur.Err())
	}
	if err = cur.All(ctx, &docs); err != nil {
		return docs, fmt.Errorf(ErrMsgDecode, err)
	}
	return docs, nil
}

func (*Collection[T]) ObjectID(id string) (primitive.ObjectID, error) {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return objectID, fmt.Errorf(ErrMsgFromHex, err)
	}
	return objectID, nil
}

func (c *Collection[T]) NormalizeFilter(filter any) (data any, err error) {
	c.Log.Debug("normalize filter")

	if _id, ok := filter.(string); ok {
		filter, err = c.ObjectID(_id)
		if err != nil {
			return
		}
	}
	if objectID, ok := filter.(primitive.ObjectID); ok {
		return bson.M{"_id": objectID}, nil
	}

	switch filter.(type) {
	case bson.A, bson.D, bson.E, bson.M:
		return filter, nil
	}

	return nil, ErrNormalizeFilter
}

func (c *Collection[T]) ToM(doc T) (bson.M, error) {
	c.Log.Debug("marshal document")
	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document due to error: %w", err)
	}

	c.Log.Debug("unmarshal document")
	var m bson.M
	if err = bson.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document due to error: %w", err)
	}

	return m, nil
}
