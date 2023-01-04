package server

import (
	"context"
	"net"
	"net/http"

	"golang.org/x/exp/slog"
)

const defaultAddr = "127.0.0.1:8080"

type Server struct {
	addr       string
	ctx        context.Context
	components []MonitoredComponent
	logger     *slog.Logger

	http *http.Server
}

func New(opts ...Option) *Server {
	server := &Server{
		addr:       defaultAddr,
		ctx:        context.Background(),
		components: []MonitoredComponent{},
		logger:     slog.Default(),
	}

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
		if addr != "" {
			s.addr = addr
		}
	}
}

func WithContext(ctx context.Context) Option {
	return func(s *Server) {
		if ctx != nil {
			s.ctx = ctx
		}
	}
}

func WithMonitoredComponents(components ...MonitoredComponent) Option {
	return func(s *Server) {
		if components != nil {
			s.components = components
		}
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) {
		if logger != nil {
			s.logger = logger
		}
	}
}
