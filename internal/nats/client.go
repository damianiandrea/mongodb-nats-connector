package nats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/nats-io/nats.go"

	"github.com/damianiandrea/mongodb-nats-connector/internal/server"
)

const (
	defaultName = "nats"
)

var (
	ErrClientDisconnected = errors.New("could not reach nats: connection closed")
)

type Client interface {
	server.NamedMonitor
	io.Closer

	AddStream(ctx context.Context, opts *AddStreamOptions) error
	Publish(ctx context.Context, opts *PublishOptions) error
}

type AddStreamOptions struct {
	StreamName string
}

type PublishOptions struct {
	Subj  string
	MsgId string
	Data  []byte
}

var _ Client = &DefaultClient{}

type DefaultClient struct {
	url    string
	name   string
	logger *slog.Logger

	conn *nats.Conn
	js   nats.JetStreamContext
}

func NewDefaultClient(opts ...ClientOption) (*DefaultClient, error) {
	c := &DefaultClient{
		name:   defaultName,
		logger: slog.Default(),
	}

	for _, opt := range opts {
		opt(c)
	}

	conn, err := nats.Connect(c.url,
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			c.logger.Error("disconnected from nats", "err", err)
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			c.logger.Info("reconnected to nats", "url", conn.ConnectedUrlRedacted())
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			c.logger.Info("nats connection closed")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect to nats: %v", err)
	}
	c.conn = conn

	js, _ := conn.JetStream()
	c.js = js

	c.logger.Info("connected to nats", "url", conn.ConnectedUrlRedacted())
	return c, nil
}

func (c *DefaultClient) Name() string {
	return c.name
}

func (c *DefaultClient) Monitor(_ context.Context) error {
	if closed := c.conn.IsClosed(); closed {
		return ErrClientDisconnected
	}
	return nil
}

func (c *DefaultClient) Close() error {
	c.conn.Close()
	return nil
}

func (c *DefaultClient) AddStream(ctx context.Context, opts *AddStreamOptions) error {
	addStreamCfg := &nats.StreamConfig{
		Name:     opts.StreamName,
		Subjects: []string{fmt.Sprintf("%s.*", opts.StreamName)},
		Storage:  nats.FileStorage,
	}
	_, err := c.js.AddStream(addStreamCfg, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("could not add nats stream %v: %v", opts.StreamName, err)
	}

	c.logger.Debug("added nats stream", "streamName", opts.StreamName)
	return nil
}

func (c *DefaultClient) Publish(ctx context.Context, opts *PublishOptions) error {
	_, err := c.js.Publish(opts.Subj, opts.Data,
		nats.Context(ctx),
		nats.MsgId(opts.MsgId),
	)
	if err != nil {
		return fmt.Errorf("could not publish message %v to nats stream %v: %v", opts.Data, opts.Subj, err)
	}
	
	c.logger.Debug("published message", "subj", opts.Subj, "data", string(opts.Data))
	return nil
}

type ClientOption func(*DefaultClient)

func WithNatsUrl(url string) ClientOption {
	return func(c *DefaultClient) {
		if url != "" {
			c.url = url
		}
	}
}

func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *DefaultClient) {
		if logger != nil {
			c.logger = logger
		}
	}
}
