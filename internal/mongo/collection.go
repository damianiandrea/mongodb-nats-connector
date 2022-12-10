package mongo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CollectionCreator struct {
	wrapped *Client
}

func NewCollectionCreator(client *Client) *CollectionCreator {
	return &CollectionCreator{wrapped: client}
}

func (c *CollectionCreator) CreateCollection(ctx context.Context, opt *CreateCollectionOption) error {
	db := c.wrapped.client.Database(opt.DbName)
	collNames, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: opt.CollName}})
	if err != nil {
		return fmt.Errorf("could not list mongo collection names: %v", err)
	}

	// creates the collection if it does not exist
	if len(collNames) == 0 {
		log.Printf("creating mongodb collection %v", opt.CollName)
		mongoOpt := options.CreateCollection()
		if opt.Capped {
			mongoOpt.SetCapped(true).SetSizeInBytes(opt.SizeInBytes)
		}
		if err := db.CreateCollection(ctx, opt.CollName, mongoOpt); err != nil {
			return fmt.Errorf("could not create mongo collection %v: %v", opt.CollName, err)
		}
	}

	// enables change stream pre and post images
	if opt.ChangeStreamPreAndPostImages {
		err = db.RunCommand(ctx, bson.D{{Key: "collMod", Value: opt.CollName},
			{Key: "changeStreamPreAndPostImages", Value: bson.D{{Key: "enabled", Value: true}}}}).Err()
		if err != nil {
			return fmt.Errorf("could not enable changeStreamPreAndPostImages on mongo collection %v: %v",
				opt.CollName, err)
		}
	}
	return nil
}

type CreateCollectionOption struct {
	DbName                       string
	CollName                     string
	Capped                       bool
	SizeInBytes                  int64
	ChangeStreamPreAndPostImages bool
}

type CollectionWatcher struct {
	wrapped *Client

	changeStreamHandler ChangeStreamHandler
}

func NewCollectionWatcher(client *Client, opts ...CollectionWatcherOption) *CollectionWatcher {
	w := &CollectionWatcher{wrapped: client}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

func (w *CollectionWatcher) WatchCollection(ctx context.Context, opt *WatchCollectionOption) error {
	resumeTokensDb := w.wrapped.client.Database(opt.ResumeTokensDbName)
	resumeTokensColl := resumeTokensDb.Collection(opt.ResumeTokensCollName)

	findOneOptions := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
	resumeToken := resumeTokensColl.FindOne(ctx, bson.D{}, findOneOptions)
	previousChangeEvent := &changeEvent{}
	if err := resumeToken.Decode(previousChangeEvent); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("could not fetch or decode resume token: %v", err)
	}

	changeStreamOptions := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetFullDocumentBeforeChange(options.WhenAvailable)

	if previousChangeEvent.Id.Data != "" {
		log.Printf("resuming from %v", previousChangeEvent.Id.Data)
		changeStreamOptions.SetResumeAfter(bson.D{{Key: "_data", Value: previousChangeEvent.Id.Data}})
	}

	watchedDb := w.wrapped.client.Database(opt.WatchedDbName)
	watchedColl := watchedDb.Collection(opt.WatchedCollName)

	cs, err := watchedColl.Watch(ctx, mongo.Pipeline{}, changeStreamOptions)
	if err != nil {
		return fmt.Errorf("could not watch mongo collection %v: %v", watchedColl.Name(), err)
	}
	log.Printf("watching collection %v", watchedColl.Name())

	for cs.Next(ctx) {
		event := &changeEvent{}
		if err = cs.Decode(event); err != nil {
			return fmt.Errorf("could not decode mongo change stream: %v", err)
		}

		json, err := bson.MarshalExtJSON(cs.Current, false, false)
		if err != nil {
			return fmt.Errorf("could not marshal mongo change stream from bson: %v", err)
		}
		log.Printf("received change event: %v", string(json))

		subj := fmt.Sprintf("%s.%s", strings.ToUpper(watchedColl.Name()), event.OperationType)
		if err = w.changeStreamHandler(subj, event.Id.Data, json); err != nil {
			// nats error: current change stream must be retried.
			// does not save current resume token, stops the connector.
			// connector will resume from the previous token upon restart.
			return fmt.Errorf("could not publish to nats stream: %v", err)
		}
		log.Printf("published change stream %v on subj %v", string(json), subj)

		if _, err := resumeTokensColl.InsertOne(ctx, event); err != nil {
			// change event has been published but token insertion failed.
			// connector will resume from the previous token upon restart publishing a duplicate change event.
			// the duplicate change event will be discarded by consumers because of the nats msg id.
			return fmt.Errorf("could not insert resume token: %v", err)
		}
	}

	log.Printf("stopped watching collection %v", watchedColl.Name())
	return cs.Close(context.Background())
}

type CollectionWatcherOption func(*CollectionWatcher)

type ChangeStreamHandler func(subj, msgId string, data []byte) error

func WithChangeStreamHandler(csHandler ChangeStreamHandler) CollectionWatcherOption {
	return func(w *CollectionWatcher) {
		w.changeStreamHandler = csHandler
	}
}

type WatchCollectionOption struct {
	WatchedDbName        string
	WatchedCollName      string
	ResumeTokensDbName   string
	ResumeTokensCollName string
}

type changeEvent struct {
	Id            changeEventId `bson:"_id"`
	OperationType string        `bson:"operationType"`
}

type changeEventId struct {
	Data string `bson:"_data"`
}
