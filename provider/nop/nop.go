package nop

import (
	"context"

	"github.com/botchris/go-pubsub"
)

// NewBroker returns a new NO-OP broker instance
func NewBroker() pubsub.Broker {
	return &broker{}
}

type broker struct{}

func (b *broker) Publish(_ context.Context, _ pubsub.Topic, _ interface{}) error {
	return nil
}

func (b *broker) Subscribe(_ context.Context, _ pubsub.Topic, _ *pubsub.Subscriber) error {
	return nil
}

func (b *broker) Unsubscribe(_ context.Context, _ pubsub.Topic, _ *pubsub.Subscriber) error {
	return nil
}

func (b *broker) Subscriptions(_ context.Context) (map[pubsub.Topic][]*pubsub.Subscriber, error) {
	return map[pubsub.Topic][]*pubsub.Subscriber{}, nil
}

func (b *broker) Shutdown(_ context.Context) error {
	return nil
}
