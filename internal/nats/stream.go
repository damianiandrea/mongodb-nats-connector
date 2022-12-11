package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"golang.org/x/exp/slog"
)

type StreamAdder struct {
	wrapped *Client
	logger  *slog.Logger
}

func NewStreamAdder(client *Client, logger *slog.Logger) *StreamAdder {
	return &StreamAdder{
		wrapped: client,
		logger:  logger,
	}
}

func (a *StreamAdder) AddStream(streamName string) error {
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

type StreamPublisher struct {
	wrapped *Client
	logger  *slog.Logger
}

func NewStreamPublisher(client *Client, logger *slog.Logger) *StreamPublisher {
	return &StreamPublisher{
		wrapped: client,
		logger:  logger,
	}
}

func (p *StreamPublisher) Publish(subj, msgId string, data []byte) error {
	if _, err := p.wrapped.js.Publish(subj, data, nats.MsgId(msgId)); err != nil {
		return fmt.Errorf("could not publish message %v to nats stream %v: %v", data, subj, err)
	}
	p.logger.Debug("published message", "subj", subj, "data", string(data))
	return nil
}
