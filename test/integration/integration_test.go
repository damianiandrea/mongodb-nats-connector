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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/damianiandrea/go-mongo-nats-connector/test"
)

var (
	mongoUri             = os.Getenv("MONGO_URI")
	natsUrl              = os.Getenv("NATS_URL")
	reconnectionAttempts = os.Getenv("RECONNECTION_ATTEMPTS")
	connectorUrl         = os.Getenv("CONNECTOR_URL")

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
		response, err := http.Get(fmt.Sprintf("%s/healthz", connectorUrl))
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

func TestHealthzEndpoint(t *testing.T) {
	response, err := http.Get(fmt.Sprintf("%s/healthz", connectorUrl))
	healthRes := &healthResponse{}

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, json.NewDecoder(response.Body).Decode(healthRes))
	require.Equal(t, healthRes.Status, "UP")
	require.Equal(t, healthRes.Components.Mongo.Status, "UP")
	require.Equal(t, healthRes.Components.Nats.Status, "UP")
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

func TestStreamsWereCreatedAndUseFileStorage(t *testing.T) {
	streamsCh := natsJs.Streams()
	streams := make(map[string]*nats.StreamInfo, 0)
	for stream := range streamsCh {
		streams[stream.Config.Name] = stream
	}

	require.NotNil(t, streams["COLL1"])
	require.Contains(t, streams["COLL1"].Config.Subjects, "COLL1.*")
	require.Equal(t, streams["COLL1"].Config.Storage, nats.FileStorage)
	require.NotNil(t, streams["COLL2"])
	require.Contains(t, streams["COLL2"].Config.Subjects, "COLL2.*")
	require.Equal(t, streams["COLL2"].Config.Storage, nats.FileStorage)
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

	event := &changeEvent{}
	require.NoError(t, json.Unmarshal(msg.Data, event))
	require.NotEmpty(t, event.Id.Data)
	require.Equal(t, event.OperationType, "insert")
	require.Equal(t, event.FullDocument.Message, "hi")

	tokensDb := mongoClient.Database("resume-tokens")
	tokensColl1 := tokensDb.Collection("coll1")
	test.Await(t, 5*time.Second, func() bool {
		return lastResumeTokenIsUpdated(tokensColl1, event)
	})

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll1.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		_, err = tokensColl1.DeleteMany(context.Background(), bson.D{})
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

	event := &changeEvent{}
	require.NoError(t, json.Unmarshal(msg.Data, event))
	require.NotEmpty(t, event.Id.Data)
	require.Equal(t, event.OperationType, "update")
	require.Equal(t, event.FullDocument.Message, "bye")

	tokensDb := mongoClient.Database("resume-tokens")
	tokensColl1 := tokensDb.Collection("coll1")
	test.Await(t, 5*time.Second, func() bool {
		return lastResumeTokenIsUpdated(tokensColl1, event)
	})

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll1.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		_, err = tokensColl1.DeleteMany(context.Background(), bson.D{})
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

	event := &changeEvent{}
	require.NoError(t, json.Unmarshal(msg.Data, event))
	require.NotEmpty(t, event.Id.Data)
	require.Equal(t, event.OperationType, "delete")
	require.Equal(t, event.FullDocumentBeforeChange.Message, "hi")

	tokensDb := mongoClient.Database("resume-tokens")
	tokensColl1 := tokensDb.Collection("coll1")
	test.Await(t, 5*time.Second, func() bool {
		return lastResumeTokenIsUpdated(tokensColl1, event)
	})

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll1.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		_, err = tokensColl1.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		require.NoError(t, natsJs.PurgeStream("COLL1"))
	})
}

func lastResumeTokenIsUpdated(tokensColl *mongo.Collection, event *changeEvent) bool {
	opt := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
	lastToken := &changeEvent{}
	if err := tokensColl.FindOne(context.Background(), bson.D{}, opt).Decode(lastToken); err != nil {
		return false
	}
	return strings.Compare(event.Id.Data, lastToken.Id.Data) == 0
}

type healthResponse struct {
	Status     string     `json:"status"`
	Components components `json:"components"`
}

type components struct {
	Mongo mongoComponent `json:"mongo"`
	Nats  natsComponent  `json:"nats"`
}

type mongoComponent struct {
	Status string `json:"status"`
}

type natsComponent struct {
	Status string `json:"status"`
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
	Id                       changeEventId `json:"_id" bson:"_id"`
	OperationType            string        `json:"operationType"`
	FullDocument             fullDocument  `json:"fullDocument"`
	FullDocumentBeforeChange fullDocument  `json:"fullDocumentBeforeChange"`
}

type changeEventId struct {
	Data string `json:"_data" bson:"_data"`
}

type fullDocument struct {
	Message string `json:"message"`
}
