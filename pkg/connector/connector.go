package connector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/damianiandrea/mongodb-nats-connector/internal/mongo"
	"github.com/damianiandrea/mongodb-nats-connector/internal/nats"
	"github.com/damianiandrea/mongodb-nats-connector/internal/prometheus"
	"github.com/damianiandrea/mongodb-nats-connector/internal/server"
)

const (
	defaultLogLevel                     = slog.LevelInfo
	defaultChangeStreamPreAndPostImages = false
	defaultTokensDbName                 = "resume-tokens"
	defaultTokensCollCapped             = false
	defaultTokensCollSizeInBytes        = 0
)

var (
	ErrDbNameMissing          = errors.New("invalid option: `dbName` is missing")
	ErrCollNameMissing        = errors.New("invalid option: `collName` is missing")
	ErrInvalidCollSizeInBytes = errors.New("invalid option: `collSizeInBytes` must be greater than 0")
	ErrInvalidDbAndCollNames  = errors.New("invalid option: `dbName` and `tokensDbName` cannot be the same if `collName` and `tokensCollName` are the same")
)

// The Connector type represents a connector between MongoDB and NATS.
type Connector struct {

	// options represents the Connector's options.
	options Options

	// logger represents the Connector's logger.
	logger *slog.Logger

	// server represents the HTTP server used by the Connector.
	server *server.Server
}

// New creates a new Connector.
// The given options will override its default configuration.
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
	c.logger = slog.New(slog.NewJSONHandler(os.Stdout, loggerOpts))

	registerer := prometheus.DefaultRegisterer()

	if c.options.mongoClient == nil {
		mongoRegisterer := prometheus.NewMongoRegisterer(registerer)
		mongoClient, err := mongo.NewDefaultClient(
			mongo.WithMongoUri(c.options.mongoUri),
			mongo.WithLogger(c.logger),
			mongo.WithEventListeners(
				mongo.OnCmdStartedEvent(mongoRegisterer.IncMongoCmdStarted),
				mongo.OnCmdSucceededEvent(mongoRegisterer.ObserveMongoCmdSucceeded),
				mongo.OnCmdFailedEvent(mongoRegisterer.ObserveMongoCmdFailed),
			),
		)
		if err != nil {
			return nil, err
		}
		c.options.mongoClient = mongoClient
	}

	if c.options.natsClient == nil {
		natsRegisterer := prometheus.NewNatsRegisterer(registerer)
		natsClient, err := nats.NewDefaultClient(
			nats.WithNatsUrl(c.options.natsUrl),
			nats.WithLogger(c.logger),
			nats.WithEventListeners(
				nats.OnMsgPublishedEvent(natsRegisterer.ObserveNatsMsgPublished),
				nats.OnMsgFailedEvent(natsRegisterer.ObserveNatsMsgFailed),
			),
		)
		if err != nil {
			return nil, err
		}
		c.options.natsClient = natsClient
	}

	c.options.ctx, c.options.stop = signal.NotifyContext(c.options.ctx, syscall.SIGINT, syscall.SIGTERM)

	c.server = server.New(
		server.WithAddr(c.options.serverAddr),
		server.WithContext(c.options.ctx),
		server.WithNamedMonitors(c.options.mongoClient, c.options.natsClient),
		server.WithLogger(c.logger),
		server.WithMetricsHandler(prometheus.HTTPHandler()),
	)

	return c, nil
}

// Run runs the Connector.
// It performs the following operations:
//
//	For each configured collection to be watched:
//		- It creates the given collection on MongoDB, if it does not already exist
//		- It creates the resume tokens collection for the given collection on MongoDB, if it does not already exist
//		- It creates the given stream on NATS, if it does not already exist
//		- Spins up a goroutine to watch the given collection
//	It runs an HTTP server in its own goroutine.
//	It runs another goroutine that will perform graceful shutdown once the Connector's context is cancelled.
func (c *Connector) Run() error {
	defer c.cleanup()

	group, groupCtx := errgroup.WithContext(c.options.ctx)

	for _, coll := range c.options.collections {
		createWatchedCollOpts := &mongo.CreateCollectionOptions{
			DbName:                       coll.dbName,
			CollName:                     coll.collName,
			ChangeStreamPreAndPostImages: coll.changeStreamPreAndPostImages,
		}
		if err := c.options.mongoClient.CreateCollection(groupCtx, createWatchedCollOpts); err != nil {
			return err
		}

		createResumeTokensCollOpts := &mongo.CreateCollectionOptions{
			DbName:      coll.tokensDbName,
			CollName:    coll.tokensCollName,
			Capped:      coll.tokensCollCapped,
			SizeInBytes: coll.tokensCollSizeInBytes,
		}
		if err := c.options.mongoClient.CreateCollection(groupCtx, createResumeTokensCollOpts); err != nil {
			return err
		}

		addStreamOpts := &nats.AddStreamOptions{StreamName: coll.streamName}
		if err := c.options.natsClient.AddStream(groupCtx, addStreamOpts); err != nil {
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
					return c.options.natsClient.Publish(ctx, publishOpts)
				},
			}
			return c.options.mongoClient.WatchCollection(groupCtx, watchCollOpts) // blocking call
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
	c.closeClient(c.options.mongoClient)
	c.closeClient(c.options.natsClient)
	c.options.stop()
}

func (c *Connector) closeClient(closer io.Closer) {
	if err := closer.Close(); err != nil {
		c.logger.Error("could not close client", "err", err)
	}
}

// Options represents the possible options to be applied to a Connector.
type Options struct {

	// logLevel represents the Connector's log level.
	// Can be set to 'info', 'debug', 'warn', or 'error'.
	logLevel slog.Level

	// mongoUri represents the Connector's MongoDB URI.
	mongoUri string

	// mongoClient represents the MongoDB client used by the Connector to connect to MongoDB.
	mongoClient mongo.Client

	// natsUrl represents the Connector's NATS URL.
	natsUrl string

	// natsClient represents the NATS client used by the Connector to connect to NATS.
	natsClient nats.Client

	// ctx represents the Connector's context.
	ctx  context.Context
	stop context.CancelFunc

	// serverAddr represents the Connector's HTTP server address.
	serverAddr string

	// collections represents a slice containing the collections to be watched, with their own configuration.
	collections []*collection
}

func getDefaultOptions() Options {
	return Options{
		logLevel:    defaultLogLevel,
		ctx:         context.Background(),
		collections: make([]*collection, 0),
	}
}

// Option is used to configure the Connector.
type Option func(*Options) error

// WithLogLevel sets the Connector's log level.
func WithLogLevel(logLevel string) Option {
	return func(o *Options) error {
		switch strings.ToLower(logLevel) {
		case "debug":
			o.logLevel = slog.LevelDebug
		case "warn":
			o.logLevel = slog.LevelWarn
		case "error":
			o.logLevel = slog.LevelError
		case "info":
			o.logLevel = slog.LevelInfo
		}
		return nil
	}
}

// WithMongoUri sets the Connector's MongoDB URI.
func WithMongoUri(mongoUri string) Option {
	return func(o *Options) error {
		if mongoUri != "" {
			o.mongoUri = mongoUri
		}
		return nil
	}
}

// withMongoClient sets the Connector's MongoDB client implementation.
// Used for testing.
func withMongoClient(mongoClient mongo.Client) Option {
	return func(o *Options) error {
		if mongoClient != nil {
			o.mongoClient = mongoClient
		}
		return nil
	}
}

// WithNatsUrl sets the Connector's NATS URL.
func WithNatsUrl(natsUrl string) Option {
	return func(o *Options) error {
		if natsUrl != "" {
			o.natsUrl = natsUrl
		}
		return nil
	}
}

// withNatsClient sets the Connector's NATS client implementation.
// Used for testing.
func withNatsClient(natsClient nats.Client) Option {
	return func(o *Options) error {
		if natsClient != nil {
			o.natsClient = natsClient
		}
		return nil
	}
}

// WithContext sets the Connector's context.
func WithContext(ctx context.Context) Option {
	return func(o *Options) error {
		if ctx != nil {
			o.ctx = ctx
		}
		return nil
	}
}

// WithServerAddr sets the Connector's HTTP server address.
func WithServerAddr(serverAddr string) Option {
	return func(o *Options) error {
		if serverAddr != "" {
			o.serverAddr = serverAddr
		}
		return nil
	}
}

// WithCollection configures a collection to be watched by the Connector, with the given options.
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

// CollectionOption is used to configure a MongoDB collection to be watched.
type CollectionOption func(*collection) error

// WithChangeStreamPreAndPostImages enables MongoDB's changeStreamPreAndPostImages configuration.
//
// Deprecated: will be removed in future versions. Set this configuration directly on MongoDB instead.
func WithChangeStreamPreAndPostImages() CollectionOption {
	return func(c *collection) error {
		c.changeStreamPreAndPostImages = true
		return nil
	}
}

// WithTokensDbName sets the name of the MongoDB database that will store the resume tokens collection for the
// collection to be watched.
func WithTokensDbName(tokensDbName string) CollectionOption {
	return func(c *collection) error {
		if tokensDbName != "" {
			c.tokensDbName = tokensDbName
		}
		return nil
	}
}

// WithTokensCollName sets the name of the MongoDB collection that will store the resume tokens for the collection to
// be watched.
func WithTokensCollName(tokensCollName string) CollectionOption {
	return func(c *collection) error {
		if tokensCollName != "" {
			c.tokensCollName = tokensCollName
		}
		return nil
	}
}

// WithTokensCollCapped sets the MongoDB collection that will store the resume tokens for the collection to be watched
// as capped, with the given size.
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

// WithStreamName sets the NATS stream name, where the MongoDB change events will be published for the collection to be
// watched.
func WithStreamName(streamName string) CollectionOption {
	return func(c *collection) error {
		if streamName != "" {
			c.streamName = streamName
		}
		return nil
	}
}
