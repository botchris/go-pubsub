package util

import (
	"context"

	"github.com/botchris/go-pubsub"
)

// Subscription convenience definition used to represent a subscription within
// a broker implementation.
type Subscription struct {
	// Ctx controls the life cycle of the subscription. This is usually mapped
	// to broker's internal context. This allows to implement graceful shutdown
	// mechanisms when broker decides to stop.
	Ctx context.Context

	// Stop is used to signal the subscription to stop. This is usually invoked
	// by the broker during graceful shutdown or during unsubscription
	// operations.
	Stop context.CancelFunc

	// Topic is the topic the subscription is listening to.
	Topic pubsub.Topic

	// Options are the options used to create the subscription.
	Options *pubsub.SubscribeOptions

	// Handler is the handler to be called when a message is received.
	Handler pubsub.Subscriber
}

// NewSubscription builds a new subscription. Given context should be the
// broker's internal context, this allows to implement graceful shutdown.
func NewSubscription(
	ctx context.Context,
	topic pubsub.Topic,
	options *pubsub.SubscribeOptions,
	handler pubsub.Subscriber,
) *Subscription {
	ctx, cancel := context.WithCancel(ctx)

	return &Subscription{
		Ctx:     ctx,
		Stop:    cancel,
		Topic:   topic,
		Options: options,
		Handler: handler,
	}
}
