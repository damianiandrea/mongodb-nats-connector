package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/exp/slog"
)

type Client struct {
	uri string

	client *mongo.Client
	logger *slog.Logger
}

func NewClient(logger *slog.Logger, opts ...ClientOption) (*Client, error) {
	c := &Client{logger: logger}

	for _, opt := range opts {
		opt(c)
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(c.uri))
	if err != nil {
		return nil, fmt.Errorf("could not connect to mongodb: %v", err)
	}
	c.client = client

	if err = c.Ping(context.Background()); err != nil {
		return nil, err
	}

	c.logger.Info("connected to mongodb", "uri", c.uri)
	return c, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("could not ping mongodb: %v", err)
	}
	return nil
}

func (c *Client) Close() error {
	if err := c.client.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("could not close mongodb client: %v", err)
	}
	return nil
}

type ClientOption func(*Client)

func WithMongoUri(uri string) ClientOption {
	return func(c *Client) {
		c.uri = uri
	}
}
