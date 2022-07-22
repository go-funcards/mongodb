package mongodb

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ErrorUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		res, err := handler(ctx, req)
		return res, normalizeError(err)
	}
}

func ErrorStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		return normalizeError(err)
	}
}

func normalizeError(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := status.FromError(err); !ok {
		if errors.Is(err, mongo.ErrNoDocuments) {
			err = status.Error(codes.NotFound, err.Error())
		} else if mongo.IsDuplicateKeyError(err) {
			err = status.Error(codes.AlreadyExists, err.Error())
		} else if mongo.IsTimeout(err) {
			err = status.Error(codes.DeadlineExceeded, err.Error())
		} else {
			err = status.Error(codes.Internal, err.Error())
		}
	}

	return err
}
