package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"

	"github.com/damianiandrea/go-mongo-nats-connector/test"
)

func TestNew(t *testing.T) {
	t.Run("should create server with defaults", func(t *testing.T) {
		srv := New()

		require.Equal(t, "127.0.0.1:8080", srv.addr)
		require.Equal(t, context.Background(), srv.ctx)
		require.Empty(t, srv.monitors)
		require.Equal(t, slog.Default(), srv.logger)
	})
	t.Run("should create server with the configured options", func(t *testing.T) {
		addr := "127.0.0.1:8085"
		ctx := context.TODO()
		cmpUp := &testComponent{name: "cmp_up", err: nil}
		cmpDown := &testComponent{name: "cmp_down", err: errors.New("not reachable")}
		logger := slog.New(slog.NewJSONHandler(os.Stdout))

		srv := New(
			WithAddr(addr),
			WithContext(ctx),
			WithNamedMonitors(cmpUp, cmpDown),
			WithLogger(logger),
		)

		require.Equal(t, addr, srv.addr)
		require.Equal(t, ctx, srv.ctx)
		require.Contains(t, srv.monitors, cmpUp)
		require.Contains(t, srv.monitors, cmpDown)
		require.Equal(t, logger, srv.logger)
	})
}

func TestServer_Run(t *testing.T) {
	t.Run("should create and run server and successfully call its health endpoint", func(t *testing.T) {
		cmpUp := &testComponent{name: "cmp_up", err: nil}
		cmpDown := &testComponent{name: "cmp_down", err: errors.New("not reachable")}

		srv := New(
			WithNamedMonitors(cmpUp, cmpDown),
		)

		// start server
		go start(srv)

		// stop server when done
		defer stop(srv)

		var res *http.Response
		err := test.Await(5*time.Second, func() bool {
			healthRes, err := healthcheck(srv)
			res = healthRes
			return err == nil
		})
		require.NoError(t, err)
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
	})
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
