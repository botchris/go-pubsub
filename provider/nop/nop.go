package nop

import (
	"context"

	"github.com/botchris/go-pubsub"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
)

// NewBroker returns a new broker instance that does nothing.
func NewBroker() pubsub.Broker {
	return &broker{}
}

type broker struct{}

func (b *broker) Publish(_ context.Context, _ pubsub.Topic, _ interface{}) error {
	return nil
}

func (b *broker) Subscribe(_ context.Context, topic pubsub.Topic, handler pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	opts := pubsub.NewSubscribeOptions(option...)

	return pubsub.NewSubscription(uuid.New(), topic, handler, nil, opts), nil
}

func (b *broker) Shutdown(_ context.Context) error {
	return nil
}
