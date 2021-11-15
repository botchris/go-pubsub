package pubsub

import (
	"context"
	"reflect"
)

// PublishInterceptor provides a hook to intercept each message before it gets published.
type PublishInterceptor func(ctx context.Context, next PublishHandler) PublishHandler

// PublishHandler wraps a call to publish for interception.
type PublishHandler func(ctx context.Context, topic Topic, m interface{}) error

// SubscriberInterceptor provides a hook to intercept each message before it gets delivered to subscribers.
//
// Please note that interception occurs for each delivery operation.
// For instance, if two subscribers `S1` and `S2` receives the same message `M`, then interception logic will be
// triggered twice for the same message `M`.
type SubscriberInterceptor func(ctx context.Context, next SubscribeMessageHandler) SubscribeMessageHandler

// SubscribeMessageHandler defines the handler invoked by SubscriberInterceptor before a message is delivered to a
// particular subscriber.
type SubscribeMessageHandler func(ctx context.Context, s *Subscriber, topic Topic, m interface{}) error

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
	// no interceptor, then just deliver
	if md.opts.subscribeInterceptor == nil {
		return md.opts.provider.Subscribe(ctx, topic, subscriber)
	}

	originalCallable := subscriber.handlerFunc
	newCallable := reflect.ValueOf(func(ctx context.Context, m interface{}) error {
		next := func(ctx context.Context, s *Subscriber, topic Topic, m interface{}) error {
			// pass to the origin handling function
			args := []reflect.Value{
				reflect.ValueOf(ctx),
				reflect.ValueOf(m),
			}

			if out := originalCallable.Call(args); out[0].Interface() != nil {
				return out[0].Interface().(error)
			}

			return nil
		}

		mw := md.opts.subscribeInterceptor(ctx, next)

		return mw(ctx, subscriber, topic, m)
	})

	subscriber.handlerFunc = newCallable

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
