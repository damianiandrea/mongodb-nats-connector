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
)

const (
	defaultLogLevel                     = slog.InfoLevel
	defaultChangeStreamPreAndPostImages = false
	defaultTokensDbName                 = "resume-tokens"
	defaultTokensCollCapped             = false
	defaultTokensCollSizeInBytes        = 0
)

var (
	ErrDbNameMissing          = errors.New("invalid config: `dbName` is missing")
	ErrCollNameMissing        = errors.New("invalid config: `collName` is missing")
	ErrInvalidCollSizeInBytes = errors.New("invalid config: `collSizeInBytes` must be greater than 0")
	ErrInvalidDbAndCollNames  = errors.New("invalid config: `dbName` and `tokensDbName` cannot be the same if `collName` and `tokensCollName` are the same")
)

type Connector struct {
	options Options

	logger      *slog.Logger
	mongoClient *mongo.Client
	natsClient  *nats.Client
	server      *server.Server
}

func New(opts ...Option) (*Connector, error) {
	c := &Connector{
		options: getDefaultOptions(),
	}

	for _, opt := range opts {
		if err := opt(&c.options); err != nil {
			return nil, err
		}
	}

	loggerOpts := &slog.HandlerOptions{Level: c.options.logLevel}
	c.logger = slog.New(loggerOpts.NewJSONHandler(os.Stdout))

	if mongoClient, err := mongo.NewClient(
		mongo.WithMongoUri(c.options.mongoUri),
		mongo.WithLogger(c.logger),
	); err != nil {
		return nil, err
	} else {
		c.mongoClient = mongoClient
	}

	if natsClient, err := nats.NewClient(
		nats.WithNatsUrl(c.options.natsUrl),
		nats.WithLogger(c.logger),
	); err != nil {
		return nil, err
	} else {
		c.natsClient = natsClient
	}

	c.options.ctx, c.options.stop = signal.NotifyContext(c.options.ctx, syscall.SIGINT, syscall.SIGTERM)

	c.server = server.New(
		server.WithAddr(c.options.serverAddr),
		server.WithContext(c.options.ctx),
		server.WithNamedMonitors(c.mongoClient, c.natsClient),
		server.WithLogger(c.logger),
	)

	return c, nil
}

func (c *Connector) Run() error {
	defer c.cleanup()

	group, groupCtx := errgroup.WithContext(c.options.ctx)

	for _, _coll := range c.options.collections {
		coll := _coll // to avoid unexpected behavior

		createWatchedCollOpts := &mongo.CreateCollectionOptions{
			DbName:                       coll.dbName,
			CollName:                     coll.collName,
			ChangeStreamPreAndPostImages: coll.changeStreamPreAndPostImages,
		}
		if err := c.mongoClient.CreateCollection(groupCtx, createWatchedCollOpts); err != nil {
			return err
		}

		createResumeTokensCollOpts := &mongo.CreateCollectionOptions{
			DbName:      coll.tokensDbName,
			CollName:    coll.tokensCollName,
			Capped:      coll.tokensCollCapped,
			SizeInBytes: coll.tokensCollSizeInBytes,
		}
		if err := c.mongoClient.CreateCollection(groupCtx, createResumeTokensCollOpts); err != nil {
			return err
		}

		addStreamOpts := &nats.AddStreamOptions{StreamName: coll.streamName}
		if err := c.natsClient.AddStream(groupCtx, addStreamOpts); err != nil {
			return err
		}

		group.Go(func() error {
			watchCollOpts := &mongo.WatchCollectionOptions{
				WatchedDbName:          coll.dbName,
				WatchedCollName:        coll.collName,
				ResumeTokensDbName:     coll.tokensDbName,
				ResumeTokensCollName:   coll.tokensCollName,
				ResumeTokensCollCapped: coll.tokensCollCapped,
				StreamName:             coll.streamName,
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
	c.options.stop()
}

func (c *Connector) closeClient(closer io.Closer) {
	if err := closer.Close(); err != nil {
		c.logger.Error("could not close client", err)
	}
}

type Options struct {
	logLevel    slog.Level
	mongoUri    string
	natsUrl     string
	ctx         context.Context
	stop        context.CancelFunc
	serverAddr  string
	collections []*collection
}

func getDefaultOptions() Options {
	return Options{
		logLevel:    defaultLogLevel,
		ctx:         context.Background(),
		collections: make([]*collection, 0),
	}
}

type Option func(*Options) error

func WithLogLevel(logLevel string) Option {
	return func(o *Options) error {
		switch strings.ToLower(logLevel) {
		case "debug":
			o.logLevel = slog.DebugLevel
		case "warn":
			o.logLevel = slog.WarnLevel
		case "error":
			o.logLevel = slog.ErrorLevel
		case "info":
			o.logLevel = slog.InfoLevel
		}
		return nil
	}
}

func WithMongoUri(mongoUri string) Option {
	return func(o *Options) error {
		if mongoUri != "" {
			o.mongoUri = mongoUri
		}
		return nil
	}
}

func WithNatsUrl(natsUrl string) Option {
	return func(o *Options) error {
		if natsUrl != "" {
			o.natsUrl = natsUrl
		}
		return nil
	}
}

func WithContext(ctx context.Context) Option {
	return func(o *Options) error {
		if ctx != nil {
			o.ctx = ctx
		}
		return nil
	}
}

func WithServerAddr(serverAddr string) Option {
	return func(o *Options) error {
		if serverAddr != "" {
			o.serverAddr = serverAddr
		}
		return nil
	}
}

func WithCollection(dbName, collName string, opts ...CollectionOption) Option {
	return func(o *Options) error {
		if dbName == "" {
			return ErrDbNameMissing
		}
		if collName == "" {
			return ErrCollNameMissing
		}
		coll := &collection{
			dbName:                       dbName,
			collName:                     collName,
			changeStreamPreAndPostImages: defaultChangeStreamPreAndPostImages,
			tokensDbName:                 defaultTokensDbName,
			tokensCollName:               collName,
			tokensCollCapped:             defaultTokensCollCapped,
			tokensCollSizeInBytes:        defaultTokensCollSizeInBytes,
			streamName:                   strings.ToUpper(collName),
		}
		for _, opt := range opts {
			if err := opt(coll); err != nil {
				return err
			}
		}
		if strings.EqualFold(coll.dbName, coll.tokensDbName) &&
			strings.EqualFold(coll.collName, coll.tokensCollName) {
			return ErrInvalidDbAndCollNames
		}
		o.collections = append(o.collections, coll)
		return nil
	}
}

type collection struct {
	dbName                       string
	collName                     string
	changeStreamPreAndPostImages bool
	tokensDbName                 string
	tokensCollName               string
	tokensCollCapped             bool
	tokensCollSizeInBytes        int64
	streamName                   string
}

type CollectionOption func(*collection) error

func WithChangeStreamPreAndPostImages() CollectionOption {
	return func(c *collection) error {
		c.changeStreamPreAndPostImages = true
		return nil
	}
}

func WithTokensDbName(tokensDbName string) CollectionOption {
	return func(c *collection) error {
		if tokensDbName != "" {
			c.tokensDbName = tokensDbName
		}
		return nil
	}
}

func WithTokensCollName(tokensCollName string) CollectionOption {
	return func(c *collection) error {
		if tokensCollName != "" {
			c.tokensCollName = tokensCollName
		}
		return nil
	}
}

func WithTokensCollCapped(collSizeInBytes int64) CollectionOption {
	return func(c *collection) error {
		if collSizeInBytes <= 0 {
			return ErrInvalidCollSizeInBytes
		}
		c.tokensCollCapped = true
		c.tokensCollSizeInBytes = collSizeInBytes
		return nil
	}
}

func WithStreamName(streamName string) CollectionOption {
	return func(c *collection) error {
		if streamName != "" {
			c.streamName = streamName
		}
		return nil
	}
}
