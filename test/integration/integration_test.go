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
	connectorHost        = os.Getenv("CONNECTOR_HOST")

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
	testCollectionsWereCreated(t, "test-connector", "coll1", "coll2")
}

func TestResumeTokenCollectionsWereCreated(t *testing.T) {
	testCollectionsWereCreated(t, "resume-tokens", "coll1", "coll2")
}

func testCollectionsWereCreated(t *testing.T, dbName string, collNames ...string) {
	db := mongoClient.Database(dbName)
	opts := options.ListCollections().SetNameOnly(true)
	colls, err := db.ListCollectionNames(context.Background(), bson.D{}, opts)

	require.NoError(t, err)
	for _, collName := range collNames {
		require.Contains(t, colls, collName)
	}
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

	sub, err := natsJs.SubscribeSync("COLL1.insert", nats.OrderedConsumer())
	require.NoError(t, err)

	msg, err := sub.NextMsg(1 * time.Minute)
	require.NoError(t, err)
	require.Equal(t, "COLL1.insert", msg.Subject)

	expectedMsgData := &changeEvent{FullDocument{Message: "hi"}}
	actualMsgData := &changeEvent{}
	err = json.Unmarshal(msg.Data, actualMsgData)
	require.NoError(t, err)
	require.Equal(t, expectedMsgData, actualMsgData)
}

type changeEvent struct {
	FullDocument `json:"fullDocument"`
}

type FullDocument struct {
	Message string `json:"message"`
}
