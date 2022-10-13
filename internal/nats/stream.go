package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

type StreamAdder struct {
	wrapped *Client
}

func NewStreamAdder(client *Client) *StreamAdder {
	return &StreamAdder{wrapped: client}
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
	return nil
}

type StreamPublisher struct {
	wrapped *Client
}

func NewStreamPublisher(client *Client) *StreamPublisher {
	return &StreamPublisher{wrapped: client}
}

func (p *StreamPublisher) Publish(subj, msgId string, data []byte) error {
	if _, err := p.wrapped.js.Publish(subj, data, nats.MsgId(msgId)); err != nil {
		return fmt.Errorf("could not publish message %v to nats stream %v: %v", data, subj, err)
	}
	return nil
}
