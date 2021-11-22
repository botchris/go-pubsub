package recovery

import (
	"context"

	"github.com/botchris/go-pubsub"
)

// HandlerFunc is a function that recovers from the panic `p` by returning
// an `error` message
type HandlerFunc func(ctx context.Context, p interface{}) error

type middleware struct {
	pubsub.Broker
	handler HandlerFunc
}

// NewRecoveryMiddleware returns a new middleware that recovers from panics when
// publishing to topics or delivering to handlers.
func NewRecoveryMiddleware(parent pubsub.Broker, handler HandlerFunc) pubsub.Broker {
	return &middleware{
		Broker:  parent,
		handler: handler,
	}
}

func (mw middleware) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
	defer func(ctx context.Context) {
		if p := recover(); p != nil {
			err = mw.handler(ctx, p)
		}
	}(ctx)

	err = mw.Broker.Publish(ctx, topic, m)

	return
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, h pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	s := &handler{
		Handler: h,
		handler: mw.handler,
	}

	return mw.Broker.Subscribe(ctx, topic, s, option...)
}
