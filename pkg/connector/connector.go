package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"

	"github.com/damianiandrea/mongodb-nats-connector/internal/mongo"
	"github.com/damianiandrea/mongodb-nats-connector/internal/nats"
	"github.com/damianiandrea/mongodb-nats-connector/internal/server"
	"github.com/damianiandrea/mongodb-nats-connector/pkg/config"
)

const (
	defaultChangeStreamPreAndPostImages = false
	defaultTokensDbName                 = "resume-tokens"
	defaultTokensCollCapped             = true
	defaultTokensCollSizeInBytes        = 4096
)

type Connector struct {
	cfg         *config.Config
	logger      *slog.Logger
	mongoClient *mongo.Client
	natsClient  *nats.Client
	ctx         context.Context
	stop        context.CancelFunc
	server      *server.Server
}

func New(cfg *config.Config) (*Connector, error) {
	if err := validateAndSetDefaults(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}

	logLevel := convertLogLevel(cfg.Connector.Log.Level)
	loggerOpts := &slog.HandlerOptions{Level: logLevel}
	logger := slog.New(loggerOpts.NewJSONHandler(os.Stdout))

	mongoClient, err := mongo.NewClient(
		mongo.WithMongoUri(cfg.Connector.Mongo.Uri),
		mongo.WithLogger(logger),
	)
	if err != nil {
		return nil, err
	}

	natsClient, err := nats.NewClient(
		nats.WithNatsUrl(cfg.Connector.Nats.Url),
		nats.WithLogger(logger),
	)
	if err != nil {
		return nil, err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	srv := server.New(
		server.WithAddr(cfg.Connector.Server.Addr),
		server.WithContext(ctx),
		server.WithNamedMonitors(mongoClient, natsClient),
		server.WithLogger(logger),
	)

	return &Connector{
		ctx:         ctx,
		stop:        stop,
		cfg:         cfg,
		logger:      logger,
		mongoClient: mongoClient,
		natsClient:  natsClient,
		server:      srv,
	}, nil
}

func (c *Connector) Run() error {
	defer c.cleanup()

	group, groupCtx := errgroup.WithContext(c.ctx)

	for _, _coll := range c.cfg.Connector.Collections {
		coll := _coll // to avoid unexpected behavior
		createWatchedCollOpts := &mongo.CreateCollectionOptions{
			DbName:                       coll.DbName,
			CollName:                     coll.CollName,
			ChangeStreamPreAndPostImages: *coll.ChangeStreamPreAndPostImages,
		}
		if err := c.mongoClient.CreateCollection(groupCtx, createWatchedCollOpts); err != nil {
			return err
		}

		createResumeTokensCollOpts := &mongo.CreateCollectionOptions{
			DbName:      coll.TokensDbName,
			CollName:    coll.TokensCollName,
			Capped:      *coll.TokensCollCapped,
			SizeInBytes: *coll.TokensCollSizeInBytes,
		}
		if err := c.mongoClient.CreateCollection(groupCtx, createResumeTokensCollOpts); err != nil {
			return err
		}

		addStreamOpts := &nats.AddStreamOptions{StreamName: coll.StreamName}
		if err := c.natsClient.AddStream(groupCtx, addStreamOpts); err != nil {
			return err
		}

		group.Go(func() error {
			watchCollOpts := &mongo.WatchCollectionOptions{
				WatchedDbName:          coll.DbName,
				WatchedCollName:        coll.CollName,
				ResumeTokensDbName:     coll.TokensDbName,
				ResumeTokensCollName:   coll.TokensCollName,
				ResumeTokensCollCapped: *coll.TokensCollCapped,
				StreamName:             coll.StreamName,
				ChangeEventHandler: func(ctx context.Context, subj, msgId string, data []byte) error {
					publishOpts := &nats.PublishOptions{
						Subj:  subj,
						MsgId: msgId,
						Data:  data,
					}
					return c.natsClient.Publish(ctx, publishOpts)
				},
			}
			return c.mongoClient.WatchCollection(groupCtx, watchCollOpts) // blocking call
		})
	}

	group.Go(func() error {
		return c.server.Run()
	})

	group.Go(func() error {
		<-groupCtx.Done()
		return c.server.Close()
	})

	return group.Wait()
}

func (c *Connector) cleanup() {
	c.closeClient(c.mongoClient)
	c.closeClient(c.natsClient)
	c.stop()
}

func (c *Connector) closeClient(closer io.Closer) {
	if err := closer.Close(); err != nil {
		c.logger.Error("could not close client", err)
	}
}

func validateAndSetDefaults(cfg *config.Config) error {
	if cfg.Connector.Log.Level == "" {
		cfg.Connector.Log.Level = os.Getenv("LOG_LEVEL")
	}

	if cfg.Connector.Mongo.Uri == "" {
		cfg.Connector.Mongo.Uri = os.Getenv("MONGO_URI")
	}

	if cfg.Connector.Nats.Url == "" {
		cfg.Connector.Nats.Url = os.Getenv("NATS_URL")
	}

	if cfg.Connector.Server.Addr == "" {
		cfg.Connector.Server.Addr = os.Getenv("SERVER_ADDR")
	}

	for _, coll := range cfg.Connector.Collections {
		if coll.DbName == "" {
			return errors.New("dbName property is missing")
		}
		if coll.CollName == "" {
			return errors.New("collName property is missing")
		}
		if strings.EqualFold(coll.DbName, coll.TokensDbName) &&
			strings.EqualFold(coll.CollName, coll.TokensCollName) {
			return fmt.Errorf("cannot store tokens in the same db and collection of the collection to be watched")
		}
		if coll.ChangeStreamPreAndPostImages == nil {
			defVal := defaultChangeStreamPreAndPostImages
			coll.ChangeStreamPreAndPostImages = &defVal
		}
		if coll.TokensDbName == "" {
			coll.TokensDbName = defaultTokensDbName
		}
		// if missing, use the coll name
		if coll.TokensCollName == "" {
			coll.TokensCollName = coll.CollName
		}
		if coll.TokensCollCapped == nil {
			defVal := defaultTokensCollCapped
			coll.TokensCollCapped = &defVal
		}
		if coll.TokensCollSizeInBytes == nil {
			var defVal int64 = defaultTokensCollSizeInBytes
			coll.TokensCollSizeInBytes = &defVal
		}
		// if missing, use the uppercase of the coll name
		if coll.StreamName == "" {
			coll.StreamName = strings.ToUpper(coll.CollName)
		}
	}

	return nil
}

func convertLogLevel(logLevel string) slog.Level {
	switch strings.ToLower(logLevel) {
	case "debug":
		return slog.DebugLevel
	case "warn":
		return slog.WarnLevel
	case "error":
		return slog.ErrorLevel
	case "info":
		fallthrough
	default:
		return slog.InfoLevel
	}
}
