package mongodb

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const ErrMsgClient = "failed to create mongodb client"

func GetClient(ctx context.Context, uri string, log zerolog.Logger) *mongo.Client {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal().Err(err).Msg(ErrMsgClient)
	}
	return client
}
