package recover

import (
	"context"
	"fmt"

	"github.com/botchris/go-pubsub"
)

// RecoveryHandlerFunc is a function that recovers from the panic `p` by returning an `error`.
type RecoveryHandlerFunc func(ctx context.Context, p interface{}) error

type middleware struct {
	pubsub.Broker
	handler RecoveryHandlerFunc
}

// NewRecoveryMiddleware returns a new middleware that recovers from panics when
// publishing to topics or delivering to subscribers.
func NewRecoveryMiddleware(parent pubsub.Broker, handler RecoveryHandlerFunc) pubsub.Broker {
	return &middleware{
		Broker:  parent,
		handler: handler,
	}
}

func (mw middleware) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
	defer func(ctx context.Context) {
		if r := recover(); r != nil {
			err = recoverFrom(ctx, r, "pubsub: publisher panic\n", mw.handler)
		}
	}(ctx)

	err = mw.Broker.Publish(ctx, topic, m)

	return
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, sub pubsub.Subscriber) error {
	s := &subscriber{
		Subscriber: sub,
		handler:    mw.handler,
	}

	return mw.Broker.Subscribe(ctx, topic, s)
}

func (mw middleware) Unsubscribe(ctx context.Context, topic pubsub.Topic, subscriber pubsub.Subscriber) error {
	return mw.Broker.Unsubscribe(ctx, topic, subscriber)
}

func recoverFrom(ctx context.Context, p interface{}, wrap string, r RecoveryHandlerFunc) error {
	if r == nil {
		return fmt.Errorf("%s: %s", p, wrap)
	}

	return r(ctx, p)
}
