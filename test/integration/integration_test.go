//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
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
	mongoUri     = os.Getenv("MONGO_URI")
	natsUrl      = os.Getenv("NATS_URL")
	connectorUrl = os.Getenv("CONNECTOR_URL")

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

	mustWaitForConnector(10 * time.Second)

	code := m.Run()

	if err := client.Disconnect(ctx); err != nil {
		log.Fatalf("could not close mongodb client: %v", err)
	}
	natsConn.Close()
	os.Exit(code)
}

func mustWaitForConnector(time time.Duration) {
	timeout, cancel := context.WithTimeout(context.Background(), time)
	defer cancel()
	for {
		select {
		case <-timeout.Done():
			log.Fatal("time exhausted: could not reach connector")
		default:
			response, err := http.Get(fmt.Sprintf("%s/healthz", connectorUrl))
			if err == nil && response.StatusCode == http.StatusOK {
				return
			}
		}
	}
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

func TestResumeTokenCollectionsWereCreated(t *testing.T) {
	db := mongoClient.Database("resume-tokens")
	colls, err := db.ListCollections(context.Background(), bson.D{})
	actualColls := make([]mongoColl, 0)

	require.NoError(t, err)
	require.NoError(t, colls.All(context.Background(), &actualColls))
	require.Contains(t, actualColls, mongoColl{Name: "coll1", Options: mongoCollOptions{Capped: true, Size: 4096}})
	require.Contains(t, actualColls, mongoColl{Name: "coll2", Options: mongoCollOptions{Capped: false, Size: 0}})
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
	testMongoInsertIsPublishedToNats(t, "coll1")
}

func TestMongoInsertIsPublishedToNatsUsingCappedResumeTokenColl(t *testing.T) {
	testMongoInsertIsPublishedToNats(t, "coll2")
}

func testMongoInsertIsPublishedToNats(t *testing.T, testColl string) {
	db := mongoClient.Database("test-connector")
	coll := db.Collection(testColl)

	result, err := coll.InsertOne(context.Background(), bson.D{{Key: "message", Value: "hi"}})
	require.NoError(t, err)
	require.NotNil(t, result.InsertedID)

	testStream := strings.ToUpper(testColl)
	subj := fmt.Sprintf("%s.insert", testStream)
	sub, err := natsJs.SubscribeSync(subj, nats.DeliverLastPerSubject())
	require.NoError(t, err)

	event := &changeEvent{}
	require.Eventually(t, func() bool {
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			return false
		}
		if err = json.Unmarshal(msg.Data, event); err != nil {
			return false
		}
		return event.Id.Data != "" && event.OperationType == "insert" && event.FullDocument.Message == "hi"
	}, 5*time.Second, 100*time.Millisecond)

	tokensDb := mongoClient.Database("resume-tokens")
	tokensColl := tokensDb.Collection(testColl)
	require.Eventually(t, func() bool {
		return lastResumeTokenIsUpdated(tokensColl, event)
	}, 5*time.Second, 100*time.Millisecond)

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		_, err = tokensColl.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		require.NoError(t, natsJs.PurgeStream(testStream))
	})
}

func TestMongoUpdateIsPublishedToNats(t *testing.T) {
	testMongoUpdateIsPublishedToNats(t, "coll1")
}

func TestMongoUpdateIsPublishedToNatsUsingCappedResumeTokenColl(t *testing.T) {
	testMongoUpdateIsPublishedToNats(t, "coll2")
}

func testMongoUpdateIsPublishedToNats(t *testing.T, testColl string) {
	db := mongoClient.Database("test-connector")
	coll := db.Collection(testColl)

	result, err := coll.InsertOne(context.Background(), bson.D{{Key: "message", Value: "hi"}})
	require.NoError(t, err)
	require.NotNil(t, result.InsertedID)

	filter := bson.D{{Key: "_id", Value: result.InsertedID}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "message", Value: "bye"}}}}
	_, err = coll.UpdateOne(context.Background(), filter, update)
	require.NoError(t, err)

	testStream := strings.ToUpper(testColl)
	subj := fmt.Sprintf("%s.update", testStream)
	sub, err := natsJs.SubscribeSync(subj, nats.DeliverLastPerSubject())
	require.NoError(t, err)

	event := &changeEvent{}
	require.Eventually(t, func() bool {
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			return false
		}
		if err = json.Unmarshal(msg.Data, event); err != nil {
			return false
		}
		return event.Id.Data != "" && event.OperationType == "update" && event.FullDocument.Message == "bye"
	}, 5*time.Second, 100*time.Millisecond)

	tokensDb := mongoClient.Database("resume-tokens")
	tokensColl := tokensDb.Collection(testColl)
	require.Eventually(t, func() bool {
		return lastResumeTokenIsUpdated(tokensColl, event)
	}, 5*time.Second, 100*time.Millisecond)

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		_, err = tokensColl.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		require.NoError(t, natsJs.PurgeStream(testStream))
	})
}

func TestMongoDeleteIsPublishedToNats(t *testing.T) {
	testMongoDeleteIsPublishedToNats(t, "coll1")
}

func TestMongoDeleteIsPublishedToNatsUsingCappedResumeTokenColl(t *testing.T) {
	testMongoDeleteIsPublishedToNats(t, "coll2")
}

func testMongoDeleteIsPublishedToNats(t *testing.T, testColl string) {
	db := mongoClient.Database("test-connector")
	coll := db.Collection(testColl)

	result, err := coll.InsertOne(context.Background(), bson.D{{Key: "message", Value: "hi"}})
	require.NoError(t, err)
	require.NotNil(t, result.InsertedID)

	filter := bson.D{{Key: "_id", Value: result.InsertedID}}
	_, err = coll.DeleteOne(context.Background(), filter)
	require.NoError(t, err)

	testStream := strings.ToUpper(testColl)
	subj := fmt.Sprintf("%s.delete", testStream)
	sub, err := natsJs.SubscribeSync(subj, nats.DeliverLastPerSubject())
	require.NoError(t, err)

	event := &changeEvent{}
	require.Eventually(t, func() bool {
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			return false
		}
		if err = json.Unmarshal(msg.Data, event); err != nil {
			return false
		}
		return event.Id.Data != "" && event.OperationType == "delete" && event.FullDocumentBeforeChange.Message == "hi"
	}, 5*time.Second, 100*time.Millisecond)

	tokensDb := mongoClient.Database("resume-tokens")
	tokensColl := tokensDb.Collection(testColl)
	require.Eventually(t, func() bool {
		return lastResumeTokenIsUpdated(tokensColl, event)
	}, 5*time.Second, 100*time.Millisecond)

	t.Cleanup(func() {
		require.NoError(t, sub.Unsubscribe())
		_, err := coll.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		_, err = tokensColl.DeleteMany(context.Background(), bson.D{})
		require.NoError(t, err)
		require.NoError(t, natsJs.PurgeStream(testStream))
	})
}

func lastResumeTokenIsUpdated(tokensColl *mongo.Collection, event *changeEvent) bool {
	opt := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
	lastResumeToken := &resumeToken{}
	if err := tokensColl.FindOne(context.Background(), bson.D{}, opt).Decode(lastResumeToken); err != nil {
		return false
	}
	return strings.Compare(event.Id.Data, lastResumeToken.Value) == 0
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

type resumeToken struct {
	Value string `bson:"value"`
}
