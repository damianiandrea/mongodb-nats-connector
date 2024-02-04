//go:build integration

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/damianiandrea/mongodb-nats-connector/test/harness"
)

func TestMongoUpdateIsPublishedToNats(t *testing.T) {
	ctx := context.Background()
	h := harness.New(t, harness.FromEnv())

	h.MustStartContainer(ctx, harness.Connector)
	t.Cleanup(func() {
		h.MustStopContainer(ctx, harness.Connector)
		assert.NoError(t, h.MongoClient.Database("test-connector").Drop(ctx))
		assert.NoError(t, h.MongoClient.Database("resume-tokens").Drop(ctx))
		assert.NoError(t, h.NatsJs.PurgeStream("COLL1"))
		assert.NoError(t, h.NatsJs.PurgeStream("COLL2"))
	})

	h.MustWaitForConnector(10 * time.Second)

	testMongoUpdateIsPublishedToNats := func(t *testing.T, h *harness.Harness, testColl string) {
		insertedID := h.MustMongoInsertOne(ctx, "test-connector", testColl, bson.D{{Key: "message", Value: "hi"}})

		filter := bson.D{{Key: "_id", Value: insertedID}}
		update := bson.D{{Key: "$set", Value: bson.D{{Key: "message", Value: "bye"}}}}
		h.MustMongoUpdateOne(ctx, "test-connector", testColl, filter, update)

		streamName := strings.ToUpper(testColl)
		msg := h.MustNatsSubscribeNextMsg(fmt.Sprintf("%s.update", streamName), 5*time.Second)

		event := &harness.ChangeEvent{}
		require.NoError(t, json.Unmarshal(msg.Data, event))
		require.NotEmpty(t, event.Id.Data)
		require.Equal(t, event.Id.Data, msg.Header.Get(nats.MsgIdHdr))
		require.Equal(t, event.OperationType, "update")
		require.Equal(t, event.FullDocument.Message, "bye")
		if harness.MustGetMongoMajorVersion(t) < 6 {
			require.Nil(t, event.FullDocumentBeforeChange)
		} else {
			require.Equal(t, event.FullDocumentBeforeChange.Message, "hi")
		}

		lastResumeToken := &harness.ResumeToken{}
		h.MustMongoFindOne(ctx, "resume-tokens", testColl, bson.D{}, bson.D{{Key: "$natural", Value: -1}}, lastResumeToken)
		require.Equal(t, event.Id.Data, lastResumeToken.Value)
	}

	t.Run("capped resume tokens collection", func(t *testing.T) {
		testMongoUpdateIsPublishedToNats(t, h, "coll1")
	})

	t.Run("uncapped resume tokens collection", func(t *testing.T) {
		testMongoUpdateIsPublishedToNats(t, h, "coll2")
	})
}
