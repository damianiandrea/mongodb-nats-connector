//go:build integration

package faultinjection

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/damianiandrea/mongodb-nats-connector/test/harness"
)

func TestRestartConnector(t *testing.T) {
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

	var (
		maxMsgs = 100
		idCh    = make(chan string, maxMsgs)
		wg      = &sync.WaitGroup{}
	)
	h.MustMongoBackgroundInsertN(maxMsgs, "test-connector", "coll1", wg, idCh)

	h.MustStopContainer(ctx, harness.Connector)
	time.Sleep(2 * time.Second)
	h.MustStartContainer(ctx, harness.Connector)

	h.MustWaitForConnector(10 * time.Second)

	msgs := h.MustNatsSubscribeAll("COLL1.insert", maxMsgs, 10*time.Second)
	wg.Wait()

	i := 0
	for id := range idCh {
		event := &harness.ChangeEvent{}
		require.NoError(t, json.Unmarshal(msgs[i].Data, event))
		require.Equal(t, id, event.FullDocument.Id.Hex())
		i++
	}
}
