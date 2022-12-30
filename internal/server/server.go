package server

import (
	"context"
	"net"
	"net/http"

	"golang.org/x/exp/slog"
)

type Server struct {
	addr       string
	ctx        context.Context
	components []MonitoredComponent

	http   *http.Server
	logger *slog.Logger
}

func New(logger *slog.Logger, opts ...Option) *Server {
	server := &Server{logger: logger}

	for _, opt := range opts {
		opt(server)
	}

	mux := http.NewServeMux()
	mux.Handle("/healthz", recoverer(NewHealthHandler(server.components...)))
	server.http = &http.Server{
		Addr:    server.addr,
		Handler: mux,
		BaseContext: func(l net.Listener) context.Context {
			return server.ctx
		},
	}

	return server
}

func (s *Server) Run() error {
	s.logger.Info("connector started", "addr", s.addr)
	return s.http.ListenAndServe()
}

func (s *Server) Close() error {
	s.logger.Info("connector gracefully shutting down", "addr", s.addr)
	return s.http.Shutdown(context.Background())
}

type Option func(*Server)

func WithAddr(addr string) Option {
	return func(s *Server) {
		s.addr = addr
	}
}

func WithContext(ctx context.Context) Option {
	return func(s *Server) {
		s.ctx = ctx
	}
}

func WithMonitoredComponents(components ...MonitoredComponent) Option {
	return func(s *Server) {
		s.components = components
	}
}
