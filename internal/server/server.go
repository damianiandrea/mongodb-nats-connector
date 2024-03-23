package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"
)

const defaultAddr = "127.0.0.1:8080"

type Server struct {
	addr           string
	ctx            context.Context
	monitors       []NamedMonitor
	logger         *slog.Logger
	metricsHandler http.Handler

	http *http.Server
}

func New(opts ...Option) *Server {
	s := &Server{
		addr:     defaultAddr,
		ctx:      context.Background(),
		monitors: []NamedMonitor{},
		logger:   slog.Default(),
	}

	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthCheck(s.monitors...))
	if s.metricsHandler != nil {
		mux.Handle("/metrics", s.metricsHandler)
	}
	
	s.http = &http.Server{
		Addr:    s.addr,
		Handler: recoverer(mux),
		BaseContext: func(l net.Listener) context.Context {
			return s.ctx
		},
	}

	return s
}

func (s *Server) Run() error {
	s.logger.Info("server started", "addr", s.addr)
	return s.http.ListenAndServe()
}

func (s *Server) Close() error {
	s.logger.Info("server gracefully shutting down", "addr", s.addr)
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

func WithNamedMonitors(monitors ...NamedMonitor) Option {
	return func(s *Server) {
		if monitors != nil {
			s.monitors = monitors
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

func WithMetricsHandler(metricsHandler http.Handler) Option {
	return func(s *Server) {
		if metricsHandler != nil {
			s.metricsHandler = metricsHandler
		}
	}
}
