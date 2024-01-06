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

func TestMongoInsertIsPublishedToNats(t *testing.T) {
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

	testMongoInsertIsPublishedToNats := func(t *testing.T, h *harness.Harness, testColl string) {
		_ = h.MustMongoInsertOne(ctx, "test-connector", testColl, bson.D{{Key: "message", Value: "hi"}})

		streamName := strings.ToUpper(testColl)
		msg := h.MustNatsSubscribeNextMsg(fmt.Sprintf("%s.insert", streamName), 5*time.Second)

		event := &harness.ChangeEvent{}
		require.NoError(t, json.Unmarshal(msg.Data, event))
		require.NotEmpty(t, event.Id.Data)
		require.Equal(t, event.Id.Data, msg.Header.Get(nats.MsgIdHdr))
		require.Equal(t, event.OperationType, "insert")
		require.Equal(t, event.FullDocument.Message, "hi")
		require.Nil(t, event.FullDocumentBeforeChange)

		lastResumeToken := &harness.ResumeToken{}
		h.MustMongoFindOne(ctx, "resume-tokens", testColl, bson.D{}, bson.D{{Key: "$natural", Value: -1}}, lastResumeToken)
		require.Equal(t, event.Id.Data, lastResumeToken.Value)
	}

	t.Run("capped resume tokens collection", func(t *testing.T) {
		testMongoInsertIsPublishedToNats(t, h, "coll1")
	})

	t.Run("uncapped resume tokens collection", func(t *testing.T) {
		testMongoInsertIsPublishedToNats(t, h, "coll2")
	})
}
