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

func (b *broker) Publish(ctx context.Context, m interface{}, topic pubsub.Topic) error {
	return nil
}

func (b *broker) Subscribe(ctx context.Context, subscriber *pubsub.Subscriber, topic pubsub.Topic) error {
	return nil
}

func (b *broker) Topics(ctx context.Context) ([]pubsub.Topic, error) {
	return []pubsub.Topic{}, nil
}

func (b *broker) Shutdown(ctx context.Context) {}
