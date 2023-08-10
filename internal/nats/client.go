package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
)

const (
	defaultUrl  = "nats://127.0.0.1:4222"
	defaultName = "nats"
)

var (
	ErrClientDisconnected = errors.New("could not reach nats: connection closed")
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

	js, _ := conn.JetStream()
	c.js = js

	c.logger.Info("connected to nats", "url", conn.ConnectedUrlRedacted())
	return c, nil
}

func (c *Client) Name() string {
	return c.name
}

func (c *Client) Monitor(_ context.Context) error {
	if closed := c.conn.IsClosed(); closed {
		return ErrClientDisconnected
	}
	return nil
}

func (c *Client) Close() error {
	c.conn.Close()
	return nil
}

func (c *Client) AddStream(_ context.Context, opts *AddStreamOptions) error {
	_, err := c.js.AddStream(&nats.StreamConfig{
		Name:     opts.StreamName,
		Subjects: []string{fmt.Sprintf("%s.*", opts.StreamName)},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("could not add nats stream %v: %v", opts.StreamName, err)
	}
	c.logger.Debug("added nats stream", "streamName", opts.StreamName)
	return nil
}

type AddStreamOptions struct {
	StreamName string
}

func (c *Client) Publish(_ context.Context, opts *PublishOptions) error {
	if _, err := c.js.Publish(opts.Subj, opts.Data, nats.MsgId(opts.MsgId)); err != nil {
		return fmt.Errorf("could not publish message %v to nats stream %v: %v", opts.Data, opts.Subj, err)
	}
	c.logger.Debug("published message", "subj", opts.Subj, "data", string(opts.Data))
	return nil
}

type PublishOptions struct {
	Subj  string
	MsgId string
	Data  []byte
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
