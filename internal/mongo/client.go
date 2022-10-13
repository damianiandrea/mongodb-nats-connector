package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client struct {
	uri string

	client *mongo.Client
}

func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{}

	for _, opt := range opts {
		opt(c)
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(c.uri))
	if err != nil {
		return nil, fmt.Errorf("could not connect to mongodb: %v", err)
	}
	c.client = client

	if err = client.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, fmt.Errorf("could not ping mongodb: %v", err)
	}

	return c, nil
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
