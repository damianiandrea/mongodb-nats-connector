package connector

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"

	"github.com/damianiandrea/go-mongo-nats-connector/internal/config"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/health"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/mongo"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/nats"
)

type Connector struct {
	cfg    *config.Config
	logger *slog.Logger
	server *http.Server
}

func New(cfg *config.Config) *Connector {
	logLevel := convertLogLevel(cfg.Connector.Log.Level)
	loggerOpts := &slog.HandlerOptions{Level: logLevel}
	logger := slog.New(loggerOpts.NewJSONHandler(os.Stdout))

	mux := http.NewServeMux()
	mux.Handle("/healthz", &health.Handler{})
	server := &http.Server{
		Addr:    cfg.Connector.Addr,
		Handler: mux,
	}

	return &Connector{
		cfg:    cfg,
		logger: logger,
		server: server,
	}
}

func (c *Connector) Run() error {
	mongoClient, err := mongo.NewClient(c.logger, mongo.WithMongoUri(c.cfg.Connector.Mongo.Uri))
	if err != nil {
		return err
	}
	defer closeClient(mongoClient)

	natsClient, err := nats.NewClient(c.logger, nats.WithNatsUrl(c.cfg.Connector.Nats.Url))
	if err != nil {
		return err
	}
	defer closeClient(natsClient)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	group, groupCtx := errgroup.WithContext(ctx)

	collCreator := mongo.NewCollectionCreator(mongoClient, c.logger)
	streamAdder := nats.NewStreamAdder(natsClient, c.logger)
	streamPublisher := nats.NewStreamPublisher(natsClient, c.logger)

	for _, _coll := range c.cfg.Connector.Collections {
		coll := _coll // to avoid unexpected behavior
		createWatchedCollOpts := &mongo.CreateCollectionOptions{
			DbName:                       coll.DbName,
			CollName:                     coll.CollName,
			ChangeStreamPreAndPostImages: *coll.ChangeStreamPreAndPostImages,
		}
		if err := collCreator.CreateCollection(groupCtx, createWatchedCollOpts); err != nil {
			return err
		}

		createResumeTokensCollOpts := &mongo.CreateCollectionOptions{
			DbName:      coll.TokensDbName,
			CollName:    coll.TokensCollName,
			Capped:      *coll.TokensCollCapped,
			SizeInBytes: *coll.TokensCollSize,
		}
		if err := collCreator.CreateCollection(groupCtx, createResumeTokensCollOpts); err != nil {
			return err
		}

		if err := streamAdder.AddStream(coll.StreamName); err != nil {
			return err
		}

		group.Go(func() error {
			watcher := mongo.NewCollectionWatcher(mongoClient, c.logger, mongo.WithChangeStreamHandler(streamPublisher.Publish))
			watchCollOpts := &mongo.WatchCollectionOptions{
				WatchedDbName:        coll.DbName,
				WatchedCollName:      coll.CollName,
				ResumeTokensDbName:   coll.TokensDbName,
				ResumeTokensCollName: coll.TokensCollName,
			}
			return watcher.WatchCollection(groupCtx, watchCollOpts) // blocking call
		})
	}

	group.Go(func() error {
		c.logger.Info("connector started", "addr", c.server.Addr)
		return c.server.ListenAndServe()
	})

	group.Go(func() error {
		<-groupCtx.Done()
		c.logger.Info("connector gracefully shutting down", "addr", c.server.Addr)
		return c.server.Shutdown(context.Background())
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

func closeClient(closer io.Closer) {
	if err := closer.Close(); err != nil {
		log.Printf("%v", err)
	}
}
