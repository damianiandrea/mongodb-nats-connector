package connector

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"

	"github.com/damianiandrea/go-mongo-nats-connector/internal/config"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/mongo"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/nats"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/server"
)

type Connector struct {
	ctx  context.Context
	stop context.CancelFunc

	cfg             *config.Config
	logger          *slog.Logger
	mongoClient     *mongo.Client
	collCreator     mongo.CollectionCreator
	collWatcher     mongo.CollectionWatcher
	natsClient      *nats.Client
	streamAdder     nats.StreamAdder
	streamPublisher nats.StreamPublisher
	server          *server.Server
}

func New(cfg *config.Config) (*Connector, error) {
	logLevel := convertLogLevel(cfg.Connector.Log.Level)
	loggerOpts := &slog.HandlerOptions{Level: logLevel}
	logger := slog.New(loggerOpts.NewJSONHandler(os.Stdout))

	mongoClient, err := mongo.NewClient(
		logger,
		mongo.WithMongoUri(cfg.Connector.Mongo.Uri),
	)
	if err != nil {
		return nil, err
	}
	collCreator := mongo.NewDefaultCollectionCreator(mongoClient, logger)
	collWatcher := mongo.NewDefaultCollectionWatcher(mongoClient, logger)

	natsClient, err := nats.NewClient(
		logger,
		nats.WithNatsUrl(cfg.Connector.Nats.Url),
	)
	if err != nil {
		return nil, err
	}
	streamAdder := nats.NewDefaultStreamAdder(natsClient, logger)
	streamPublisher := nats.NewDefaultStreamPublisher(natsClient, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	srv := server.New(
		logger,
		server.WithAddr(cfg.Connector.Addr),
		server.WithContext(ctx),
		server.WithMonitoredComponents(mongoClient, natsClient),
	)

	return &Connector{
		ctx:             ctx,
		stop:            stop,
		cfg:             cfg,
		logger:          logger,
		mongoClient:     mongoClient,
		collCreator:     collCreator,
		collWatcher:     collWatcher,
		natsClient:      natsClient,
		streamAdder:     streamAdder,
		streamPublisher: streamPublisher,
		server:          srv,
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
		if err := c.collCreator.CreateCollection(groupCtx, createWatchedCollOpts); err != nil {
			return err
		}

		createResumeTokensCollOpts := &mongo.CreateCollectionOptions{
			DbName:      coll.TokensDbName,
			CollName:    coll.TokensCollName,
			Capped:      *coll.TokensCollCapped,
			SizeInBytes: *coll.TokensCollSize,
		}
		if err := c.collCreator.CreateCollection(groupCtx, createResumeTokensCollOpts); err != nil {
			return err
		}

		if err := c.streamAdder.AddStream(coll.StreamName); err != nil {
			return err
		}

		group.Go(func() error {
			watchCollOpts := &mongo.WatchCollectionOptions{
				WatchedDbName:        coll.DbName,
				WatchedCollName:      coll.CollName,
				ResumeTokensDbName:   coll.TokensDbName,
				ResumeTokensCollName: coll.TokensCollName,
				StreamName:           coll.StreamName,
				ChangeStreamHandler:  c.streamPublisher.Publish,
			}
			return c.collWatcher.WatchCollection(groupCtx, watchCollOpts) // blocking call
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

func (c *Connector) cleanup() {
	closeClient(c.mongoClient)
	closeClient(c.natsClient)
	c.stop()
}

func closeClient(closer io.Closer) {
	if err := closer.Close(); err != nil {
		log.Printf("%v", err)
	}
}
