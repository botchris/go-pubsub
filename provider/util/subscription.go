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
	pubsub.Subscription

	// ctx controls the life cycle of the subscription. This is usually mapped
	// to broker's internal context. This allows to implement graceful shutdown
	// mechanisms when broker decides to stop.
	ctx context.Context

	// stop is used to signal the subscription to stop. This is usually invoked
	// by the broker during graceful shutdown or during unsubscription
	// operations.
	stop context.CancelFunc
}

func (s *subscription) Context() context.Context {
	return s.ctx
}

func (s *subscription) Stop() {
	s.stop()
}

// NewStoppableSubscription builds a new stoppable subscription. Given context
// should be the broker's internal context, this allows to implement graceful shutdown.
func NewStoppableSubscription(
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

	parent := pubsub.NewSubscription(id, topic, handler, unsubscriber, options)
	ctx, cancel := context.WithCancel(ctx)

	return &subscription{
		ctx:          ctx,
		stop:         cancel,
		Subscription: parent,
	}, nil
}
