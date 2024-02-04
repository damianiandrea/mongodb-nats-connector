package mongo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/damianiandrea/mongodb-nats-connector/internal/server"
)

const (
	defaultName = "mongo"
)

const (
	insertOperationType     = "insert"
	updateOperationType     = "update"
	replacOperationType     = "replace"
	deleteOperationType     = "delete"
	invalidateOperationType = "invalidate"
)

var publishableOperationTypes = map[string]struct{}{
	insertOperationType: {},
	updateOperationType: {},
	replacOperationType: {},
	deleteOperationType: {},
}

type Client interface {
	server.NamedMonitor
	io.Closer

	CreateCollection(ctx context.Context, opts *CreateCollectionOptions) error
	WatchCollection(ctx context.Context, opts *WatchCollectionOptions) error
}

type CreateCollectionOptions struct {
	DbName                       string
	CollName                     string
	Capped                       bool
	SizeInBytes                  int64
	ChangeStreamPreAndPostImages bool
}

type ChangeEventHandler func(ctx context.Context, subj, msgId string, data []byte) error

type WatchCollectionOptions struct {
	WatchedDbName          string
	WatchedCollName        string
	ResumeTokensDbName     string
	ResumeTokensCollName   string
	ResumeTokensCollCapped bool
	StreamName             string
	ChangeEventHandler     ChangeEventHandler
}

var _ Client = &DefaultClient{}

type DefaultClient struct {
	uri    string
	name   string
	logger *slog.Logger

	client *mongo.Client
}

func NewDefaultClient(opts ...ClientOption) (*DefaultClient, error) {
	c := &DefaultClient{
		name:   defaultName,
		logger: slog.Default(),
	}

	for _, opt := range opts {
		opt(c)
	}

	parsedUri, err := url.Parse(c.uri)
	if err != nil {
		return nil, fmt.Errorf("invalid mongodb uri: %v", err)
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(c.uri))
	if err != nil {
		return nil, fmt.Errorf("could not connect to mongodb: %v", err)
	}
	c.client = client

	c.logger.Info("connected to mongodb", "uri", parsedUri.Redacted())
	return c, nil
}

func (c *DefaultClient) Name() string {
	return c.name
}

func (c *DefaultClient) Monitor(ctx context.Context) error {
	if err := c.client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("could not reach mongodb: %v", err)
	}
	return nil
}

func (c *DefaultClient) Close() error {
	if err := c.client.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("could not close mongodb client: %v", err)
	}
	return nil
}

func (c *DefaultClient) CreateCollection(ctx context.Context, opts *CreateCollectionOptions) error {
	db := c.client.Database(opts.DbName)
	collNames, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: opts.CollName}})
	if err != nil {
		return fmt.Errorf("could not list mongo collection names: %v", err)
	}

	// creates the collection if it does not exist
	if len(collNames) == 0 {
		mongoOpt := options.CreateCollection()
		if opts.Capped {
			mongoOpt.SetCapped(true).SetSizeInBytes(opts.SizeInBytes)
		}
		if err := db.CreateCollection(ctx, opts.CollName, mongoOpt); err != nil {
			return fmt.Errorf("could not create mongo collection %v: %v", opts.CollName, err)
		}
		c.logger.Debug("created mongodb collection", "collName", opts.CollName, "dbName", opts.DbName)
	}

	// enables change stream pre and post images
	if opts.ChangeStreamPreAndPostImages {
		enablePreAndPostImages := bson.D{{Key: "collMod", Value: opts.CollName},
			{Key: "changeStreamPreAndPostImages", Value: bson.D{{Key: "enabled", Value: true}}}}
		if err = db.RunCommand(ctx, enablePreAndPostImages).Err(); err != nil {
			c.logger.Warn("could not enable changeStreamPreAndPostImages, is your MongoDB version at least 6.0?",
				"collName", opts.CollName, "err", err)
		}
	}
	return nil
}

func (c *DefaultClient) WatchCollection(ctx context.Context, opts *WatchCollectionOptions) error {

	resumeTokensDb := c.client.Database(opts.ResumeTokensDbName)
	resumeTokensColl := resumeTokensDb.Collection(opts.ResumeTokensCollName)

	watchedDb := c.client.Database(opts.WatchedDbName)
	watchedColl := watchedDb.Collection(opts.WatchedCollName)

	resume := true
	for resume {
		findOneOpts := options.FindOne()
		if opts.ResumeTokensCollCapped {
			// use natural sort for capped collections to get the last inserted resume token
			findOneOpts.SetSort(bson.D{{Key: "$natural", Value: -1}})
		} else {
			// cannot rely on natural sort for uncapped collections, sort by id instead
			findOneOpts.SetSort(bson.D{{Key: "_id", Value: -1}})
		}

		lastResumeToken := &resumeToken{}
		err := resumeTokensColl.FindOne(ctx, bson.D{}, findOneOpts).Decode(lastResumeToken)
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("could not fetch or decode resume token: %v", err)
		}

		changeStreamOpts := options.ChangeStream().
			SetFullDocument(options.UpdateLookup).
			SetFullDocumentBeforeChange(options.WhenAvailable)

		if lastResumeToken.Value != "" {
			c.logger.Debug("resuming after token", "token", lastResumeToken.Value)
			changeStreamOpts.SetResumeAfter(bson.D{{Key: "_data", Value: lastResumeToken.Value}})
		}

		cs, err := watchedColl.Watch(ctx, mongo.Pipeline{}, changeStreamOpts)
		if err != nil {
			return fmt.Errorf("could not watch mongo collection %v: %v", watchedColl.Name(), err)
		}
		c.logger.Info("watching mongodb collection", "collName", watchedColl.Name())

		for cs.Next(ctx) {
			currentResumeToken := cs.Current.Lookup("_id", "_data").StringValue()
			operationType := cs.Current.Lookup("operationType").StringValue()

			json, err := bson.MarshalExtJSON(cs.Current, false, false)
			if err != nil {
				return fmt.Errorf("could not marshal mongo change event from bson: %v", err)
			}
			c.logger.Debug("received change event", "changeEvent", string(json))

			if _, ok := publishableOperationTypes[operationType]; !ok {
				if operationType == invalidateOperationType {
					resume = false
					break
				}
				continue
			}

			subj := fmt.Sprintf("%s.%s", opts.StreamName, operationType)
			if err = opts.ChangeEventHandler(ctx, subj, currentResumeToken, json); err != nil {
				// current change event was not published.
				// current resume token will not be stored.
				// connector will resume after the previous token.
				c.logger.Error("could not publish change event", err)
				break
			}

			if _, err = resumeTokensColl.InsertOne(ctx, &resumeToken{Value: currentResumeToken}); err != nil {
				// change event has been published but token insertion failed.
				// connector will resume after the previous token, publishing a duplicate change event.
				// consumers should be able to detect and discard the duplicate change event by using the msg id.
				c.logger.Error("could not insert resume token", err)
				break
			}
		}

		c.logger.Info("stopped watching mongodb collection", "collName", watchedColl.Name())
		if err = cs.Close(context.Background()); err != nil {
			return fmt.Errorf("could not close change stream: %v", err)
		}
	}

	return nil
}

type resumeToken struct {
	Value string `bson:"value"`
}

type ClientOption func(*DefaultClient)

func WithMongoUri(uri string) ClientOption {
	return func(c *DefaultClient) {
		if uri != "" {
			c.uri = uri
		}
	}
}

func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *DefaultClient) {
		if logger != nil {
			c.logger = logger
		}
	}
}
