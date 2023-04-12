package main

import (
	"log"
	"os"

	"github.com/damianiandrea/mongodb-nats-connector/internal/config"
	"github.com/damianiandrea/mongodb-nats-connector/pkg/connector"
)

const defaultConfigFileName = "connector.yaml"

func main() {
	configFileName := getEnvOrDefault("CONFIG_FILE", defaultConfigFileName)
	cfg, err := config.Load(configFileName)
	if err != nil {
		log.Fatalf("error while loading config: %v", err)
	}

	opts := []connector.Option{
		connector.WithLogLevel(getEnvOrDefault("LOG_LEVEL", cfg.Connector.Log.Level)),
		connector.WithMongoUri(getEnvOrDefault("MONGO_URI", cfg.Connector.Mongo.Uri)),
		connector.WithNatsUrl(getEnvOrDefault("NATS_URL", cfg.Connector.Nats.Url)),
		connector.WithServerAddr(getEnvOrDefault("SERVER_ADDR", cfg.Connector.Server.Addr)),
	}
	for _, coll := range cfg.Connector.Collections {
		collOpts := []connector.CollectionOption{
			connector.WithTokensDbName(coll.TokensDbName),
			connector.WithTokensCollName(coll.TokensCollName),
			connector.WithStreamName(coll.StreamName),
		}
		if coll.ChangeStreamPreAndPostImages != nil && *coll.ChangeStreamPreAndPostImages {
			collOpts = append(collOpts, connector.WithChangeStreamPreAndPostImages())
		}
		if coll.TokensCollCapped != nil && coll.TokensCollSizeInBytes != nil && *coll.TokensCollCapped {
			collOpts = append(collOpts, connector.WithTokensCollCapped(*coll.TokensCollSizeInBytes))
		}
		opt := connector.WithCollection(coll.DbName, coll.CollName, collOpts...)
		opts = append(opts, opt)
	}

	if conn, err := connector.New(opts...); err != nil {
		log.Fatalf("could not create connector: %v", err)
	} else {
		log.Fatalf("exiting: %v", conn.Run())
	}
}

func getEnvOrDefault(env, def string) string {
	if val, found := os.LookupEnv(env); found {
		return val
	}
	return def
}
