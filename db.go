package mongodb

import (
	"context"
	"errors"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

const ErrMsgDatabase = "failed to create mongodb database"

var ErrNoDB = errors.New("database name not found in URI")

func GetDB(ctx context.Context, uri string, log zerolog.Logger) *mongo.Database {
	dbName, err := GetDBName(uri)
	if err != nil {
		log.Fatal().Err(err).Msg(ErrMsgDatabase)
	}

	return GetClient(ctx, uri, log).Database(dbName)
}

func GetDBName(uri string) (string, error) {
	cs, err := connstring.ParseAndValidate(uri)
	if err != nil {
		return "", err
	}
	if len(cs.Database) == 0 {
		return "", ErrNoDB
	}
	return cs.Database, nil
}
