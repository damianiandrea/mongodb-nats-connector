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

	"github.com/damianiandrea/go-mongo-nats-connector/internal/health"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/mongo"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/nats"
)

const defaultResumeTokensDbName = "resume-tokens"

var (
	mongoUri             = os.Getenv("MONGO_URI")
	mongoDatabase        = os.Getenv("MONGO_DATABASE")
	mongoCollectionNames = os.Getenv("MONGO_COLLECTION_NAMES")
	natsUrl              = os.Getenv("NATS_URL")
	serverAddr           = os.Getenv("SERVER_ADDR")
)

type Connector struct {
	server *http.Server
	logger *slog.Logger
}

func New() *Connector {
	mux := http.NewServeMux()
	mux.Handle("/healthz", &health.Handler{})

	server := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}
	loggerOpts := &slog.HandlerOptions{Level: slog.DebugLevel}
	logger := slog.New(loggerOpts.NewJSONHandler(os.Stdout))
	return &Connector{
		server: server,
		logger: logger,
	}
}

func (c *Connector) Run() error {
	mongoClient, err := mongo.NewClient(c.logger, mongo.WithMongoUri(mongoUri))
	if err != nil {
		return err
	}
	defer closeClient(mongoClient)

	natsClient, err := nats.NewClient(c.logger, nats.WithNatsUrl(natsUrl))
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

	collNames := strings.Split(mongoCollectionNames, ",")
	for _, collName := range collNames {

		createWatchedCollOpts := &mongo.CreateCollectionOptions{
			DbName:                       mongoDatabase,
			CollName:                     collName,
			ChangeStreamPreAndPostImages: true,
		}
		if err := collCreator.CreateCollection(groupCtx, createWatchedCollOpts); err != nil {
			return err
		}

		createResumeTokensCollOpts := &mongo.CreateCollectionOptions{
			DbName:      defaultResumeTokensDbName,
			CollName:    collName,
			Capped:      true,
			SizeInBytes: 4096,
		}
		if err := collCreator.CreateCollection(groupCtx, createResumeTokensCollOpts); err != nil {
			return err
		}

		// by default stream name is the uppercase of the coll name
		streamName := strings.ToUpper(collName)
		if err := streamAdder.AddStream(streamName); err != nil {
			return err
		}

		_collName := collName // to avoid unexpected behavior
		group.Go(func() error {
			watcher := mongo.NewCollectionWatcher(mongoClient, c.logger, mongo.WithChangeStreamHandler(streamPublisher.Publish))
			watchCollOpts := &mongo.WatchCollectionOptions{
				WatchedDbName:        mongoDatabase,
				WatchedCollName:      _collName,
				ResumeTokensDbName:   defaultResumeTokensDbName,
				ResumeTokensCollName: _collName,
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

func closeClient(closer io.Closer) {
	if err := closer.Close(); err != nil {
		log.Printf("%v", err)
	}
}
