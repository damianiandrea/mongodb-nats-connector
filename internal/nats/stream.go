package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"golang.org/x/exp/slog"
)

type StreamAdder interface {
	AddStream(streamName string) error
}

type DefaultStreamAdder struct {
	wrapped *Client
	logger  *slog.Logger
}

func NewDefaultStreamAdder(client *Client, logger *slog.Logger) *DefaultStreamAdder {
	return &DefaultStreamAdder{
		wrapped: client,
		logger:  logger,
	}
}

func (a *DefaultStreamAdder) AddStream(streamName string) error {
	_, err := a.wrapped.js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{fmt.Sprintf("%s.*", streamName)},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("could not add nats stream %v: %v", streamName, err)
	}
	a.logger.Debug("added nats stream", "streamName", streamName)
	return nil
}

type StreamPublisher interface {
	Publish(subj, msgId string, data []byte) error
}

type DefaultStreamPublisher struct {
	wrapped *Client
	logger  *slog.Logger
}

func NewDefaultStreamPublisher(client *Client, logger *slog.Logger) *DefaultStreamPublisher {
	return &DefaultStreamPublisher{
		wrapped: client,
		logger:  logger,
	}
}

func (p *DefaultStreamPublisher) Publish(subj, msgId string, data []byte) error {
	if _, err := p.wrapped.js.Publish(subj, data, nats.MsgId(msgId)); err != nil {
		return fmt.Errorf("could not publish message %v to nats stream %v: %v", data, subj, err)
	}
	p.logger.Debug("published message", "subj", subj, "data", string(data))
	return nil
}
