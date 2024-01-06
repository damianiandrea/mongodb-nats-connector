//go:build integration

package acceptance

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/damianiandrea/mongodb-nats-connector/test/harness"
)

func TestConnectorStartup(t *testing.T) {
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

	t.Run("watched collections were created", func(t *testing.T) {
		db := h.MongoClient.Database("test-connector")
		colls, err := db.ListCollectionNames(ctx, bson.D{})

		require.NoError(t, err)
		require.Contains(t, colls, "coll1")
		require.Contains(t, colls, "coll2")
	})

	t.Run("resume collections were created", func(t *testing.T) {
		db := h.MongoClient.Database("resume-tokens")
		colls, err := db.ListCollections(ctx, bson.D{})
		actualColls := make([]mongoColl, 0)

		require.NoError(t, err)
		require.NoError(t, colls.All(ctx, &actualColls))
		require.Contains(t, actualColls, mongoColl{Name: "coll1", Options: mongoCollOptions{Capped: true, Size: 4096}})
		require.Contains(t, actualColls, mongoColl{Name: "coll2", Options: mongoCollOptions{Capped: false, Size: 0}})
	})

	t.Run("streams were created", func(t *testing.T) {
		streamsCh := h.NatsJs.Streams()
		streams := make(map[string]*nats.StreamInfo)
		for stream := range streamsCh {
			streams[stream.Config.Name] = stream
		}

		require.NotNil(t, streams["COLL1"])
		require.Contains(t, streams["COLL1"].Config.Subjects, "COLL1.*")
		require.Equal(t, streams["COLL1"].Config.Storage, nats.FileStorage)
		require.NotNil(t, streams["COLL2"])
		require.Contains(t, streams["COLL2"].Config.Subjects, "COLL2.*")
		require.Equal(t, streams["COLL2"].Config.Storage, nats.FileStorage)
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
