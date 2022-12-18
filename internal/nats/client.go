package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"golang.org/x/exp/slog"
)

type Client struct {
	url string

	conn   *nats.Conn
	js     nats.JetStreamContext
	logger *slog.Logger
}

func NewClient(logger *slog.Logger, opts ...ClientOption) (*Client, error) {
	c := &Client{logger: logger}

	for _, opt := range opts {
		opt(c)
	}

	conn, err := nats.Connect(c.url)
	if err != nil {
		return nil, fmt.Errorf("could not connect to nats: %v", err)
	}
	c.conn = conn

	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("could not create nats jetstream context: %v", err)
	}
	c.js = js

	c.logger.Info("connected to nats", "url", conn.ConnectedUrlRedacted())
	return c, nil
}

func (c *Client) Close() error {
	c.conn.Close()
	return nil
}

type ClientOption func(*Client)

func WithNatsUrl(url string) ClientOption {
	return func(c *Client) {
		c.url = url
	}
}
