package nop

import (
	"context"

	"github.com/ChristopherCastro/go-pubsub"
)

// NewBroker returns a new NO-OP broker instance
func NewBroker() pubsub.Broker {
	return &broker{}
}

type broker struct{}

func (b *broker) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	return nil
}

func (b *broker) Subscribe(ctx context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	return nil
}

func (b *broker) Topics(ctx context.Context) ([]pubsub.Topic, error) {
	return []pubsub.Topic{}, nil
}

func (b *broker) Shutdown(ctx context.Context) error {
	return nil
}
