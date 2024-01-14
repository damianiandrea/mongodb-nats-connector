//go:build integration

package faultinjection

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/damianiandrea/mongodb-nats-connector/test/harness"
)

func TestRestartMongo(t *testing.T) {
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

	beforeSubscribeFunc := func() {
		h.MustStopContainer(ctx, harness.Mongo1, harness.Mongo2, harness.Mongo3)
		time.Sleep(2 * time.Second)
		h.MustStartContainer(ctx, harness.Mongo1, harness.Mongo2, harness.Mongo3)

		h.MustWaitForMongo(10 * time.Second)
	}

	h.MustVerifyMessageCorrectness(100, "test-connector", "coll1", beforeSubscribeFunc)
}
