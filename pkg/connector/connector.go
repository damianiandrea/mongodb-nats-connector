package connector

import (
	"context"
	"errors"
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
	defaultLogLevel                     = slog.InfoLevel
	defaultChangeStreamPreAndPostImages = false
	defaultTokensDbName                 = "resume-tokens"
	defaultTokensCollCapped             = true
	defaultTokensCollSizeInBytes        = 4096
)

var (
	ErrDbNameMissing         = errors.New("invalid config: `dbName` is missing")
	ErrCollNameMissing       = errors.New("invalid config: `collName` is missing")
	ErrInvalidDbAndCollNames = errors.New("invalid config: `dbName` and `tokensDbName` cannot be the same if `collName` and `tokensCollName` are the same")
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
		return nil, err
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
	if logLevel, found := os.LookupEnv("LOG_LEVEL"); found {
		cfg.Connector.Log.Level = logLevel
	}

	if mongoUri, found := os.LookupEnv("MONGO_URI"); found {
		cfg.Connector.Mongo.Uri = mongoUri
	}

	if natsUrl, found := os.LookupEnv("NATS_URL"); found {
		cfg.Connector.Nats.Url = natsUrl
	}

	if serverAddr, found := os.LookupEnv("SERVER_ADDR"); found {
		cfg.Connector.Server.Addr = serverAddr
	}

	for _, coll := range cfg.Connector.Collections {
		if coll.DbName == "" {
			return ErrDbNameMissing
		}
		if coll.CollName == "" {
			return ErrCollNameMissing
		}
		if strings.EqualFold(coll.DbName, coll.TokensDbName) &&
			strings.EqualFold(coll.CollName, coll.TokensCollName) {
			return ErrInvalidDbAndCollNames
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
		return slog.InfoLevel
	default:
		return defaultLogLevel
	}
}
