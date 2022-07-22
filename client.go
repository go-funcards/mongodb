package mongodb

import (
	"context"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const ErrMsgClient = "failed to create mongodb client"

func GetClient(uri string, log logrus.FieldLogger) *mongo.Client {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		log.WithField("error", err).Fatal(ErrMsgClient)
	}
	return client
}
