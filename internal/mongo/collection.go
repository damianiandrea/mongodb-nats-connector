package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/exp/slog"
)

type CollectionCreator interface {
	CreateCollection(ctx context.Context, opts *CreateCollectionOptions) error
}

type DefaultCollectionCreator struct {
	wrapped *Client
	logger  *slog.Logger
}

func NewDefaultCollectionCreator(client *Client, logger *slog.Logger) *DefaultCollectionCreator {
	return &DefaultCollectionCreator{
		wrapped: client,
		logger:  logger,
	}
}

func (c *DefaultCollectionCreator) CreateCollection(ctx context.Context, opts *CreateCollectionOptions) error {
	db := c.wrapped.client.Database(opts.DbName)
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
		err = db.RunCommand(ctx, bson.D{{Key: "collMod", Value: opts.CollName},
			{Key: "changeStreamPreAndPostImages", Value: bson.D{{Key: "enabled", Value: true}}}}).Err()
		if err != nil {
			return fmt.Errorf("could not enable changeStreamPreAndPostImages on mongo collection %v: %v",
				opts.CollName, err)
		}
	}
	return nil
}

type CreateCollectionOptions struct {
	DbName                       string
	CollName                     string
	Capped                       bool
	SizeInBytes                  int64
	ChangeStreamPreAndPostImages bool
}

type CollectionWatcher interface {
	WatchCollection(ctx context.Context, opts *WatchCollectionOptions) error
}

type DefaultCollectionWatcher struct {
	wrapped *Client
	logger  *slog.Logger
}

func NewDefaultCollectionWatcher(client *Client, logger *slog.Logger) *DefaultCollectionWatcher {
	return &DefaultCollectionWatcher{
		wrapped: client,
		logger:  logger,
	}
}

func (w *DefaultCollectionWatcher) WatchCollection(ctx context.Context, opts *WatchCollectionOptions) error {
	for {
		resumeTokensDb := w.wrapped.client.Database(opts.ResumeTokensDbName)
		resumeTokensColl := resumeTokensDb.Collection(opts.ResumeTokensCollName)

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
			w.logger.Debug("resuming after token", "token", lastResumeToken.Value)
			changeStreamOpts.SetResumeAfter(bson.D{{Key: "_data", Value: lastResumeToken.Value}})
		}

		watchedDb := w.wrapped.client.Database(opts.WatchedDbName)
		watchedColl := watchedDb.Collection(opts.WatchedCollName)

		cs, err := watchedColl.Watch(ctx, mongo.Pipeline{}, changeStreamOpts)
		if err != nil {
			return fmt.Errorf("could not watch mongo collection %v: %v", watchedColl.Name(), err)
		}
		w.logger.Info("watching mongodb collection", "collName", watchedColl.Name())

		for cs.Next(ctx) {
			currentResumeToken := cs.Current.Lookup("_id", "_data").StringValue()
			operationType := cs.Current.Lookup("operationType").StringValue()

			json, err := bson.MarshalExtJSON(cs.Current, false, false)
			if err != nil {
				return fmt.Errorf("could not marshal mongo change event from bson: %v", err)
			}
			w.logger.Debug("received change event", "changeEvent", string(json))

			subj := fmt.Sprintf("%s.%s", opts.StreamName, operationType)
			if err = opts.ChangeEventHandler(subj, currentResumeToken, json); err != nil {
				// current change event was not published.
				// connector will retry from the previous token.
				w.logger.Error("could not publish change event", err)
				break
			}

			if _, err = resumeTokensColl.InsertOne(ctx, &resumeToken{Value: currentResumeToken}); err != nil {
				// change event has been published but token insertion failed.
				// connector will retry from the previous token, publishing a duplicate change event.
				// consumers should be able to detect and discard the duplicate change event by using the msg id.
				w.logger.Error("could not insert resume token", err)
				break
			}
		}

		w.logger.Info("stopped watching mongodb collection", "collName", watchedColl.Name())
		if err = cs.Close(context.Background()); err != nil {
			return err
		}
	}
}

type ChangeEventHandler func(subj, msgId string, data []byte) error

type WatchCollectionOptions struct {
	WatchedDbName          string
	WatchedCollName        string
	ResumeTokensDbName     string
	ResumeTokensCollName   string
	ResumeTokensCollCapped bool
	StreamName             string
	ChangeEventHandler     ChangeEventHandler
}

type resumeToken struct {
	Value string `bson:"value"`
}
