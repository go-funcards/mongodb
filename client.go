package mongodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"time"
)

const (
	timeout = 5 * time.Second
	msg     = "failed to create mongodb client due to error: "
)

type Config struct {
	URI  string `yaml:"uri" env:"URI" env-required:"true"`
	Ping bool   `yaml:"ping" env:"PING" env-default:"false"`
}

func (cfg *Config) GetClient(ctx context.Context) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("%s%w", msg, err)
	}

	if cfg.Ping {
		err = client.Ping(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("%s%w", msg, err)
		}
	}

	return client, nil
}

func (cfg *Config) GetDatabase(ctx context.Context) (*mongo.Database, error) {
	cs, err := connstring.ParseAndValidate(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("%s%w", msg, err)
	}

	if len(cs.Database) == 0 {
		return nil, fmt.Errorf("%s%s", msg, "database name not found")
	}

	opts := options.Database()

	if len(cs.ReadConcernLevel) != 0 {
		rc := readconcern.New(readconcern.Level(cs.ReadConcernLevel))
		opts.SetReadConcern(rc)
	}

	if cs.WTimeoutSet || cs.WNumberSet || len(cs.WString) != 0 {
		var wcOpts []writeconcern.Option
		if cs.WTimeoutSet {
			wcOpts = append(wcOpts, writeconcern.WTimeout(cs.WTimeout))
		}
		if cs.WNumberSet {
			wcOpts = append(wcOpts, writeconcern.W(cs.WNumber))
		} else if len(cs.WString) != 0 {
			wcOpts = append(wcOpts, writeconcern.WTagSet(cs.WString))
		}
		opts.SetWriteConcern(writeconcern.New(wcOpts...))
	}

	if len(cs.ReadPreference) != 0 {
		rMode, err := readpref.ModeFromString(cs.ReadPreference)
		if err != nil {
			return nil, fmt.Errorf("%s%w", msg, err)
		}

		var rpOpts []readpref.Option
		if cs.MaxStalenessSet {
			rpOpts = append(rpOpts, readpref.WithMaxStaleness(cs.MaxStaleness))
		}

		rp, err := readpref.New(rMode, rpOpts...)
		if err != nil {
			return nil, fmt.Errorf("%s%w", msg, err)
		}

		opts.SetReadPreference(rp)
	}

	client, err := cfg.GetClient(ctx)
	if err != nil {
		return nil, err
	}

	return client.Database(cs.Database, opts), nil
}

func DefaultTxnOptions() *options.TransactionOptions {
	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	return options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)
}

func UseSession(ctx context.Context, client *mongo.Client, fn func(mongo.SessionContext) error) error {
	session, err := client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	if err = mongo.WithSession(ctx, session, fn); err != nil {
		if abortErr := session.AbortTransaction(ctx); abortErr != nil {
			panic(abortErr)
		}
		return err
	}
	return nil
}
