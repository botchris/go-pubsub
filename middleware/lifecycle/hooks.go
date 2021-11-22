package lifecycle

import (
	"context"

	"github.com/botchris/go-pubsub"
)

// Hooks provides callback functions to react to broker's lifecycle events.
type Hooks struct {
	// OnPublish is invoked after a message is published. The given error
	// argument is nil if the message was successfully published.
	OnPublish func(ctx context.Context, topic pubsub.Topic, m interface{}, err error)

	// OnDeliver is invoked after a message is delivered to a subscription. The
	// given error argument is nil if the message was successfully delivered.
	OnDeliver func(ctx context.Context, topic pubsub.Topic, m interface{}, err error)

	// OnShutdown is invoked after the broker completes its shutdown. The given
	// error argument is nil if the broker shutdown successfully.
	OnShutdown func(ctx context.Context, err error)
}

type hookMiddleware struct {
	pubsub.Broker
	hooks Hooks
}

// NewLifecycleMiddleware builds a new hook middleware that allows to react
// after a broker publishes a message, delivers a message or after the broker
// shuts down.
func NewLifecycleMiddleware(src pubsub.Broker, hooks Hooks) pubsub.Broker {
	return &hookMiddleware{
		Broker: src,
		hooks:  hooks,
	}
}

func (l *hookMiddleware) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	err := l.Broker.Publish(ctx, topic, m)
	if l.hooks.OnPublish != nil {
		l.hooks.OnPublish(ctx, topic, m, err)
	}

	return err
}

func (l *hookMiddleware) Subscribe(ctx context.Context, topic pubsub.Topic, h pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	if l.hooks.OnDeliver != nil {
		h = &handler{
			Handler: h,
			hook:    l.hooks.OnDeliver,
		}
	}

	return l.Broker.Subscribe(ctx, topic, h, option...)
}

func (l *hookMiddleware) Shutdown(ctx context.Context) error {
	err := l.Broker.Shutdown(ctx)
	if l.hooks.OnShutdown != nil {
		l.hooks.OnShutdown(ctx, err)
	}

	return err
}
