package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"golang.org/x/exp/slog"
)

const (
	defaultUrl  = "nats://127.0.0.1:4222"
	defaultName = "nats"
)

type Client struct {
	url    string
	name   string
	logger *slog.Logger

	conn *nats.Conn
	js   nats.JetStreamContext
}

func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{
		url:    defaultUrl,
		name:   defaultName,
		logger: slog.Default(),
	}

	for _, opt := range opts {
		opt(c)
	}

	conn, err := nats.Connect(c.url)
	if err != nil {
		return nil, fmt.Errorf("could not connect to nats: %v", err)
	}
	c.conn = conn

	if err = c.Ping(context.Background()); err != nil {
		return nil, err
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("could not create nats jetstream context: %v", err)
	}
	c.js = js

	c.logger.Info("connected to nats", "url", conn.ConnectedUrlRedacted())
	return c, nil
}

func (c *Client) Name() string {
	return c.name
}

func (c *Client) Ping(_ context.Context) error {
	if closed := c.conn.IsClosed(); closed {
		return errors.New("could not ping nats: connection closed")
	}
	return nil
}

func (c *Client) Close() error {
	c.conn.Close()
	return nil
}

type ClientOption func(*Client)

func WithNatsUrl(url string) ClientOption {
	return func(c *Client) {
		if url != "" {
			c.url = url
		}
	}
}

func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}
