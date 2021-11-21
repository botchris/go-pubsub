package nop

import (
	"context"

	"github.com/botchris/go-pubsub"
)

// NewBroker returns a new broker instance that does nothing.
func NewBroker() pubsub.Broker {
	return &broker{}
}

type broker struct{}

func (b *broker) Publish(_ context.Context, _ pubsub.Topic, _ interface{}) error {
	return nil
}

func (b *broker) Subscribe(_ context.Context, _ pubsub.Topic, _ pubsub.Subscriber, option ...pubsub.SubscribeOption) error {
	return nil
}

func (b *broker) Unsubscribe(_ context.Context, _ pubsub.Topic, _ pubsub.Subscriber) error {
	return nil
}

func (b *broker) Shutdown(_ context.Context) error {
	return nil
}
