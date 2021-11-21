package util

import (
	"context"
	"errors"

	"github.com/botchris/go-pubsub"
)

// StoppableSubscription represents a subscription that can be stopped.
type StoppableSubscription interface {
	pubsub.Subscription

	// Context returns the internal context of this subscription which controls
	// its life cycle. This is usually branched from broker's internal context,
	// and allows implementing graceful shutdown mechanisms when broker decides
	//to stop.
	Context() context.Context

	// Stop is used to signal the subscription to stop. This is usually invoked
	// by the broker during graceful shutdown or during unsubscription
	// operations.
	Stop()
}

// Subscription convenience definition used to represent a subscription within
// a broker implementation.
type subscription struct {
	// id uniquely identifies the subscription within the broker.
	id string

	// ctx controls the life cycle of the subscription. This is usually mapped
	// to broker's internal context. This allows to implement graceful shutdown
	// mechanisms when broker decides to stop.
	ctx context.Context

	// stop is used to signal the subscription to stop. This is usually invoked
	// by the broker during graceful shutdown or during unsubscription
	// operations.
	stop context.CancelFunc

	// topic is the topic the subscription is listening to.
	topic pubsub.Topic

	// options are the options used to create the subscription.
	options pubsub.SubscribeOptions

	// handler is the handler to be called when a message is received.
	handler pubsub.Handler

	// unsubscribe encapsulates the logic to unsubscribe this subscription.
	unsubscriber func() error
}

func (s *subscription) Context() context.Context {
	return s.ctx
}

func (s *subscription) Stop() {
	s.stop()
}

func (s *subscription) ID() string {
	return s.id
}

func (s *subscription) Options() pubsub.SubscribeOptions {
	return s.options
}

func (s *subscription) Topic() pubsub.Topic {
	return s.topic
}

func (s *subscription) Unsubscribe() error {
	return s.unsubscriber()
}

func (s *subscription) Handler() pubsub.Handler {
	return s.handler
}

// NewSubscription builds a new subscription. Given context should be the
// broker's internal context, this allows to implement graceful shutdown.
func NewSubscription(
	ctx context.Context,
	id string,
	topic pubsub.Topic,
	handler pubsub.Handler,
	unsubscriber func() error,
	options pubsub.SubscribeOptions,
) (StoppableSubscription, error) {
	if ctx == nil {
		return nil, errors.New("subscription context cannot be nil")
	}

	if topic.String() == "" {
		return nil, errors.New("subscription topic cannot be empty")
	}

	if handler == nil {
		return nil, errors.New("subscription handler cannot be nil")
	}

	ctx, cancel := context.WithCancel(ctx)

	return &subscription{
		ctx:          ctx,
		id:           id,
		stop:         cancel,
		topic:        topic,
		options:      options,
		handler:      handler,
		unsubscriber: unsubscriber,
	}, nil
}
