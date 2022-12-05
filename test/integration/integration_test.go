//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	mongoUri             = os.Getenv("MONGO_URI")
	natsUrl              = os.Getenv("NATS_URL")
	reconnectionAttempts = os.Getenv("RECONNECTION_ATTEMPTS")
	connectorHost        = os.Getenv("CONNECTOR_URL")

	mongoClient *mongo.Client

	natsJs nats.JetStreamContext
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUri))
	if err != nil {
		log.Fatalf("could not connect to mongodb: %v", err)
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatalf("could not ping mongodb: %v", err)
	}
	mongoClient = client

	natsConn, err := nats.Connect(natsUrl)
	if err != nil {
		log.Fatalf("could not connect to nats: %v", err)
	}
	natsJs, err = natsConn.JetStream()
	if err != nil {
		log.Fatalf("could not create nats jetstream context: %v", err)
	}

	attemptsLeft, _ := strconv.ParseUint(reconnectionAttempts, 10, 64)
	for attemptsLeft > 0 {
		response, err := http.Get(fmt.Sprintf("%s/healthz", connectorHost))
		if err == nil && response.StatusCode == http.StatusOK {
			break
		}
		attemptsLeft--
		log.Printf("connector not ready, reconnection attempts left: %d\n", attemptsLeft)
		time.Sleep(1 * time.Second)
	}
	if attemptsLeft == 0 {
		log.Fatalf("attempts exhausted: could not reach connector")
	}

	code := m.Run()

	if err := client.Disconnect(ctx); err != nil {
		log.Fatalf("could not close mongodb client: %v", err)
	}
	natsConn.Close()
	os.Exit(code)
}

func TestWatchedCollectionsWereCreated(t *testing.T) {
	db := mongoClient.Database("test-connector")
	colls, err := db.ListCollectionNames(context.Background(), bson.D{})

	require.NoError(t, err)
	require.Contains(t, colls, "coll1")
	require.Contains(t, colls, "coll2")
}

func TestResumeTokenCollectionsWereCreatedAndAreCapped(t *testing.T) {
	db := mongoClient.Database("resume-tokens")
	colls, err := db.ListCollections(context.Background(), bson.D{})
	actualColls := make([]mongoColl, 0)

	require.NoError(t, err)
	require.NoError(t, colls.All(context.Background(), &actualColls))
	require.Contains(t, actualColls, mongoColl{Name: "coll1", Options: mongoCollOptions{Capped: true, Size: 4096}})
	require.Contains(t, actualColls, mongoColl{Name: "coll2", Options: mongoCollOptions{Capped: true, Size: 4096}})
}

func TestStreamsWereCreated(t *testing.T) {
	streamsCh := natsJs.StreamNames()
	streams := make([]string, 0)
	for stream := range streamsCh {
		streams = append(streams, stream)
	}

	require.Contains(t, streams, "COLL1")
	require.Contains(t, streams, "COLL2")
}

func TestMongoInsertIsPublishedToNats(t *testing.T) {
	db := mongoClient.Database("test-connector")
	coll1 := db.Collection("coll1")

	result, err := coll1.InsertOne(context.Background(), bson.D{{Key: "message", Value: "hi"}})
	require.NoError(t, err)
	require.NotNil(t, result.InsertedID)

	sub, err := natsJs.SubscribeSync("COLL1.insert", nats.DeliverLastPerSubject())
	require.NoError(t, err)
	msg, err := sub.NextMsg(1 * time.Minute)
	require.NoError(t, err)

	expectedMsgData := &changeEvent{FullDocument{Message: "hi"}}
	actualMsgData := &changeEvent{}
	require.NoError(t, json.Unmarshal(msg.Data, actualMsgData))
	require.Equal(t, expectedMsgData, actualMsgData)

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll1.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		require.NoError(t, natsJs.PurgeStream("COLL1"))
	})
}

func TestMongoUpdateIsPublishedToNats(t *testing.T) {
	db := mongoClient.Database("test-connector")
	coll1 := db.Collection("coll1")

	result, err := coll1.InsertOne(context.Background(), bson.D{{Key: "message", Value: "hi"}})
	require.NoError(t, err)
	require.NotNil(t, result.InsertedID)

	filter := bson.D{{Key: "_id", Value: result.InsertedID}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "message", Value: "bye"}}}}
	_, err = coll1.UpdateOne(context.Background(), filter, update)
	require.NoError(t, err)

	sub, err := natsJs.SubscribeSync("COLL1.update", nats.DeliverLastPerSubject())
	require.NoError(t, err)
	msg, err := sub.NextMsg(1 * time.Minute)
	require.NoError(t, err)

	expectedMsgData := &changeEvent{FullDocument{Message: "bye"}}
	actualMsgData := &changeEvent{}
	require.NoError(t, json.Unmarshal(msg.Data, actualMsgData))
	require.Equal(t, expectedMsgData, actualMsgData)

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll1.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		require.NoError(t, natsJs.PurgeStream("COLL1"))
	})
}

func TestMongoDeleteIsPublishedToNats(t *testing.T) {
	db := mongoClient.Database("test-connector")
	coll1 := db.Collection("coll1")

	result, err := coll1.InsertOne(context.Background(), bson.D{{Key: "message", Value: "hi"}})
	require.NoError(t, err)
	require.NotNil(t, result.InsertedID)

	filter := bson.D{{Key: "_id", Value: result.InsertedID}}
	_, err = coll1.DeleteOne(context.Background(), filter)
	require.NoError(t, err)

	sub, err := natsJs.SubscribeSync("COLL1.delete", nats.DeliverLastPerSubject())
	require.NoError(t, err)
	msg, err := sub.NextMsg(1 * time.Minute)
	require.NoError(t, err)

	expectedMsgData := &changeEventBefore{FullDocument{Message: "hi"}}
	actualMsgData := &changeEventBefore{}
	require.NoError(t, json.Unmarshal(msg.Data, actualMsgData))
	require.Equal(t, expectedMsgData, actualMsgData)

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll1.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		require.NoError(t, natsJs.PurgeStream("COLL1"))
	})
}

type mongoColl struct {
	Name    string           `bson:"name"`
	Options mongoCollOptions `bson:"options"`
}

type mongoCollOptions struct {
	Capped bool  `bson:"capped"`
	Size   int64 `bson:"size"`
}

type changeEvent struct {
	FullDocument `json:"fullDocument"`
}

type changeEventBefore struct {
	FullDocument `json:"fullDocumentBeforeChange"`
}

type FullDocument struct {
	Message string `json:"message"`
}
