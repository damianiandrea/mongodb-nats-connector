package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		var (
			addr           = "127.0.0.1:8085"
			ctx            = context.TODO()
			cmpUp          = &testComponent{name: "cmp_up", err: nil}
			cmpDown        = &testComponent{name: "cmp_down", err: errors.New("not reachable")}
			logger         = slog.New(slog.NewJSONHandler(os.Stdout, nil))
			metricsHandler = &testMetricsHandler{}
		)

		srv := New(
			WithAddr(addr),
			WithContext(ctx),
			WithNamedMonitors(cmpUp, cmpDown),
			WithLogger(logger),
			WithMetricsHandler(metricsHandler),
		)

		require.Equal(t, addr, srv.addr)
		require.Equal(t, ctx, srv.ctx)
		require.Contains(t, srv.monitors, cmpUp)
		require.Contains(t, srv.monitors, cmpDown)
		require.Equal(t, logger, srv.logger)
		require.Equal(t, metricsHandler, srv.metricsHandler)
	})
}

func TestServer_Run(t *testing.T) {
	var (
		cmpUp          = &testComponent{name: "cmp_up", err: nil}
		cmpDown        = &testComponent{name: "cmp_down", err: errors.New("not reachable")}
		metricsHandler = &testMetricsHandler{}
	)

	srv := New(
		WithNamedMonitors(cmpUp, cmpDown),
		WithMetricsHandler(metricsHandler),
	)

	go func() {
		_ = srv.Run()
	}()
	t.Cleanup(func() {
		_ = srv.Close()
	})

	waitForHealthyServer := func() {
		require.Eventually(t, func() bool {
			_, err := healthcheck(srv)
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)
	}

	t.Run("should successfully call health endpoint", func(t *testing.T) {
		waitForHealthyServer()

		res, err := healthcheck(srv)
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

	t.Run("should successfully call metrics endpoint", func(t *testing.T) {
		waitForHealthyServer()

		res, err := http.Get(fmt.Sprintf("http://%s/metrics", srv.addr))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Equal(t, []byte("test metrics"), body)
	})
}

func healthcheck(srv *Server) (*http.Response, error) {
	return http.Get(fmt.Sprintf("http://%s/healthz", srv.addr))
}

type testMetricsHandler struct{}

func (h *testMetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("test metrics"))
}
