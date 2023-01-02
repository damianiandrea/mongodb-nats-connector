package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"

	"github.com/damianiandrea/go-mongo-nats-connector/test"
)

func TestServer(t *testing.T) {
	addr := "127.0.0.1:8080"
	ctx := context.Background()
	cmpUp := &testComponent{name: "cmp_up", err: nil}
	cmpDown := &testComponent{name: "cmp_down", err: errors.New("not pingable")}

	srv := New(
		slog.Default(),
		WithAddr(addr),
		WithContext(ctx),
		WithMonitoredComponents(cmpUp, cmpDown),
	)

	require.Equal(t, addr, srv.addr)
	require.Equal(t, ctx, srv.ctx)
	require.Contains(t, srv.components, cmpUp)
	require.Contains(t, srv.components, cmpDown)

	// start server
	go start(srv)

	// stop server when done
	defer stop(srv)

	var res *http.Response
	test.Await(t, 5*time.Second, func() bool {
		healthRes, err := healthcheck(srv)
		res = healthRes
		return err == nil
	})
	gotBody := healthResponse{}
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "application/json", res.Header.Get("Content-Type"))
	require.NoError(t, json.NewDecoder(res.Body).Decode(&gotBody))
	require.Equal(t, healthResponse{
		Status: UP,
		Components: map[string]monitoredComponents{
			"cmp_up":   {Status: UP},
			"cmp_down": {Status: DOWN},
		},
	}, gotBody)
}

func start(srv *Server) {
	_ = srv.Run()
}

func stop(srv *Server) {
	_ = srv.Close()
}

func healthcheck(srv *Server) (*http.Response, error) {
	return http.Get(fmt.Sprintf("http://%s/healthz", srv.addr))
}
