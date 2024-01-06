//go:build integration

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/damianiandrea/mongodb-nats-connector/test/harness"
)

func TestConnectorHealthz(t *testing.T) {
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

	response, err := http.Get(fmt.Sprintf("%s/healthz", h.ConnectorUrl))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, response.Body.Close())
	})

	healthRes := &healthResponse{}
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, json.NewDecoder(response.Body).Decode(healthRes))
	require.Equal(t, healthRes.Status, "UP")
	require.Equal(t, healthRes.Components.Mongo.Status, "UP")
	require.Equal(t, healthRes.Components.Nats.Status, "UP")
}

type healthResponse struct {
	Status     string     `json:"status"`
	Components components `json:"components"`
}

type components struct {
	Mongo component `json:"mongo"`
	Nats  component `json:"nats"`
}

type component struct {
	Status string `json:"status"`
}
