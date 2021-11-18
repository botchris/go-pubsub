package pubsub

import (
	"context"
)

// PublishInterceptor provides a hook to intercept each message before it gets published.
type PublishInterceptor func(ctx context.Context, next PublishHandler) PublishHandler

// PublishHandler wraps a call to publish for interception.
type PublishHandler func(ctx context.Context, topic Topic, msg interface{}) error

// SubscriberInterceptor provides a hook to intercept each message before it gets delivered to subscribers.
//
// Please note that interception occurs for each delivery operation.
// For instance, if two subscribers `S1` and `S2` receives the same message `M`, then interception logic will be
// triggered twice for the same message `M`.
type SubscriberInterceptor func(ctx context.Context, next SubscriberMessageHandler) SubscriberMessageHandler

// SubscriberMessageHandler defines the handler invoked by SubscriberInterceptor before a message is delivered to a
// particular subscriber.
type SubscriberMessageHandler func(ctx context.Context, s *Subscriber, topic Topic, msg interface{}) error

// broker acts as a wrapper of another broker with interception capabilities.
type middleware struct {
	opts options
}

// NewMiddlewareBroker returns a new broker with interception capabilities.
func NewMiddlewareBroker(provider Broker, opt ...Option) Broker {
	opts := options{
		provider: provider,
	}

	for _, o := range opt {
		o.apply(&opts)
	}

	c := &middleware{
		opts: opts,
	}

	chainPublisherInterceptors(c)
	chainSubscriberInterceptors(c)

	return c
}

func (md *middleware) Publish(ctx context.Context, topic Topic, m interface{}) error {
	if md.opts.publishInterceptor == nil {
		return md.opts.provider.Publish(ctx, topic, m)
	}

	mw := md.opts.publishInterceptor(ctx, func(ctx context.Context, topic Topic, m interface{}) error {
		return md.opts.provider.Publish(ctx, topic, m)
	})

	return mw(ctx, topic, m)
}

func (md *middleware) Subscribe(ctx context.Context, topic Topic, subscriber *Subscriber) error {
	if md.opts.subscribeInterceptor == nil {
		return md.opts.provider.Subscribe(ctx, topic, subscriber)
	}

	subscriber.interceptor = md.opts.subscribeInterceptor

	return md.opts.provider.Subscribe(ctx, topic, subscriber)
}

func (md *middleware) Unsubscribe(ctx context.Context, topic Topic, subscriber *Subscriber) error {
	return md.opts.provider.Unsubscribe(ctx, topic, subscriber)
}

func (md *middleware) Subscriptions(ctx context.Context) (map[Topic][]*Subscriber, error) {
	return md.opts.provider.Subscriptions(ctx)
}

func (md *middleware) Shutdown(ctx context.Context) error {
	return md.opts.provider.Shutdown(ctx)
}
