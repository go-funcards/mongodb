package mongodb

import (
	"errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

const ErrMsgDatabase = "failed to create mongodb database"

var ErrNoDB = errors.New("database name not found in URI")

func GetDB(uri string, log logrus.FieldLogger) *mongo.Database {
	dbName, err := GetDBName(uri)
	if err != nil {
		log.WithField("error", err).Fatal(ErrMsgDatabase)
	}

	return GetClient(uri, log).Database(dbName)
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
