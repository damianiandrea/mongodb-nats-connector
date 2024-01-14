//go:build integration

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	Mongo1    = "mongo1"
	Mongo2    = "mongo2"
	Mongo3    = "mongo3"
	Nats1     = "nats1"
	Nats2     = "nats2"
	Nats3     = "nats3"
	Connector = "connector"
)

type Harness struct {
	t *testing.T

	DockerClient *client.Client
	MongoClient  *mongo.Client
	NatsConn     *nats.Conn
	NatsJs       nats.JetStreamContext
	ConnectorUrl string
}

type Options struct {
	MongoUri     string
	NatsUrl      string
	ConnectorUrl string
}

func FromEnv() *Options {
	return &Options{
		MongoUri:     os.Getenv("MONGO_URI"),
		NatsUrl:      os.Getenv("NATS_URL"),
		ConnectorUrl: os.Getenv("CONNECTOR_URL"),
	}
}

func New(t *testing.T, opt *Options) *Harness {
	ctx := context.Background()
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, dockerClient.Close())
	})

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(opt.MongoUri))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, mongoClient.Disconnect(context.Background()))
	})

	err = mongoClient.Ping(ctx, readpref.Primary())
	require.NoError(t, err)

	natsConn, err := nats.Connect(opt.NatsUrl)
	require.NoError(t, err)
	t.Cleanup(func() {
		natsConn.Close()
	})

	natsJs, err := natsConn.JetStream()
	require.NoError(t, err)

	return &Harness{
		t:            t,
		DockerClient: dockerClient,
		MongoClient:  mongoClient,
		NatsConn:     natsConn,
		NatsJs:       natsJs,
		ConnectorUrl: opt.ConnectorUrl,
	}
}

func (h *Harness) MustStartContainer(ctx context.Context, names ...string) {
	for _, name := range names {
		err := h.DockerClient.ContainerStart(ctx, name, types.ContainerStartOptions{})
		require.NoError(h.t, err)
	}
}

func (h *Harness) MustStopContainer(ctx context.Context, names ...string) {
	for _, name := range names {
		err := h.DockerClient.ContainerStop(ctx, name, container.StopOptions{})
		require.NoError(h.t, err)
	}
}

func (h *Harness) MustWaitForConnector(wait time.Duration) {
	h.mustCallHealthz(h.ConnectorUrl, wait)
}

func (h *Harness) MustWaitForNats(wait time.Duration) {
	h.mustCallHealthz(fmt.Sprintf("http://%s:8222", Nats1), wait)
}

func (h *Harness) mustCallHealthz(url string, wait time.Duration) {
	healthzUrl := fmt.Sprintf("%s/healthz", url)
	cond := func() bool {
		response, err := http.Get(healthzUrl)
		return err == nil && response.StatusCode == http.StatusOK
	}
	require.Eventually(h.t, cond, wait, 50*time.Millisecond, "time exhausted: could not reach %s", healthzUrl)
}

func (h *Harness) MustWaitForMongo(wait time.Duration) {
	cond := func() bool {
		return h.MongoClient.Ping(context.Background(), readpref.PrimaryPreferred()) == nil
	}
	require.Eventually(h.t, cond, wait, 50*time.Millisecond, "time exhausted: could not reach mongo")
}

func (h *Harness) MustMongoInsertOne(ctx context.Context, dbName, collName string, doc bson.D) primitive.ObjectID {
	db := h.MongoClient.Database(dbName)
	coll := db.Collection(collName)

	result, err := coll.InsertOne(ctx, doc)
	require.NoError(h.t, err)
	require.NotNil(h.t, result.InsertedID)

	return result.InsertedID.(primitive.ObjectID)
}

func (h *Harness) MustMongoBackgroundInsertN(n int, dbName, collName string, wg *sync.WaitGroup, idCh chan string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(idCh)
		for i := 0; i < n; i++ {
			document := bson.D{{Key: "message", Value: fmt.Sprintf("test %d", i)}}
			insertedID := h.MustMongoInsertOne(context.Background(), dbName, collName, document)
			idCh <- insertedID.Hex()
		}
	}()
}

func (h *Harness) MustMongoFindOne(ctx context.Context, dbName, collName string, filter, sort bson.D, result any) {
	db := h.MongoClient.Database(dbName)
	coll := db.Collection(collName)

	opt := options.FindOne().SetSort(sort)
	require.NoError(h.t, coll.FindOne(ctx, filter, opt).Decode(result))
}

func (h *Harness) MustMongoUpdateOne(ctx context.Context, dbName, collName string, filter, update bson.D) {
	db := h.MongoClient.Database(dbName)
	coll := db.Collection(collName)

	result, err := coll.UpdateOne(ctx, filter, update)
	require.NoError(h.t, err)
	require.Equal(h.t, int64(1), result.ModifiedCount)
}

func (h *Harness) MustMongoDeleteOne(ctx context.Context, dbName, collName string, filter bson.D) {
	db := h.MongoClient.Database(dbName)
	coll := db.Collection(collName)

	result, err := coll.DeleteOne(ctx, filter)
	require.NoError(h.t, err)
	require.Equal(h.t, int64(1), result.DeletedCount)
}

func (h *Harness) MustMongoReplaceOne(ctx context.Context, dbName, collName string, filter bson.D, replacement bson.D) {
	db := h.MongoClient.Database(dbName)
	coll := db.Collection(collName)

	result, err := coll.ReplaceOne(ctx, filter, replacement)
	require.NoError(h.t, err)
	require.Equal(h.t, int64(1), result.ModifiedCount)
}

func (h *Harness) MustNatsSubscribeNextMsg(subj string, timeout time.Duration) *nats.Msg {
	sub, err := h.NatsJs.SubscribeSync(subj, nats.DeliverLastPerSubject())
	require.NoError(h.t, err)
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(timeout)
	require.NoError(h.t, err)
	return msg
}

func (h *Harness) MustNatsSubscribeAll(subj string, maxMsgs int, wait time.Duration) []*nats.Msg {
	msgCh := make(chan *nats.Msg, maxMsgs)
	defer close(msgCh)

	sub, err := h.NatsJs.ChanSubscribe(subj, msgCh, nats.DeliverAll())
	require.NoError(h.t, err)
	defer sub.Unsubscribe()

	msgs := make([]*nats.Msg, maxMsgs)

	timeout, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for i := 0; i < maxMsgs; i++ {
		select {
		case <-timeout.Done():
			break
		case msg := <-msgCh:
			msgs[i] = msg
		}
	}
	return msgs
}

func (h *Harness) MustVerifyMessageCorrectness(maxMsgs int, dbName, collName string, beforeSubscribeFunc func()) {
	var (
		idCh   = make(chan string, maxMsgs)
		wg     = &sync.WaitGroup{}
		stream = strings.ToUpper(collName)
	)
	h.MustMongoBackgroundInsertN(maxMsgs, dbName, collName, wg, idCh)

	beforeSubscribeFunc()

	msgs := h.MustNatsSubscribeAll(stream+".insert", maxMsgs, 10*time.Second)
	wg.Wait()

	i := 0
	for id := range idCh {
		event := &ChangeEvent{}
		require.NoError(h.t, json.Unmarshal(msgs[i].Data, event))
		require.Equal(h.t, id, event.FullDocument.Id.Hex())
		i++
	}
}

type ChangeEvent struct {
	Id                       ChangeEventId `json:"_id" bson:"_id"`
	OperationType            string        `json:"operationType"`
	FullDocument             *FullDocument `json:"fullDocument"`
	FullDocumentBeforeChange *FullDocument `json:"fullDocumentBeforeChange"`
}

type ChangeEventId struct {
	Data string `json:"_data" bson:"_data"`
}

type FullDocument struct {
	Id      primitive.ObjectID `json:"_id"`
	Message string             `json:"message"`
}

type ResumeToken struct {
	Value string `bson:"value"`
}
