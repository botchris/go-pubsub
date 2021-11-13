package interceptor

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/botchris/go-pubsub"
)

// PublishHandler wraps a call to publish, for interception
type PublishHandler func(ctx context.Context, m interface{}, topic pubsub.Topic) error

// SubscribeMessageHandler defines the handler invoked by SubscriberInterceptor before a message is delivered to a particular subscriber.
type SubscribeMessageHandler func(ctx context.Context, s *pubsub.Subscriber, m interface{}) error

// PublishInterceptor provides a hook to intercept each message before it gets published.
type PublishInterceptor func(ctx context.Context, next PublishHandler) PublishHandler

// SubscriberInterceptor provides a hook to intercept each message before it gets delivered to subscribers.
//
// Please note that interception occurs for each delivery operation.
// For instance, if two subscribers `S1` and `S2` receives the same message `M`, then interception logic will be
// triggered twice for the same message `M`.
type SubscriberInterceptor func(ctx context.Context, next SubscribeMessageHandler) SubscribeMessageHandler

// NewBroker returns a new broker with interception capabilities.
func NewBroker(provider pubsub.Broker, opt ...Option) pubsub.Broker {
	opts := options{
		provider: provider,
	}

	for _, o := range opt {
		o.apply(&opts)
	}

	c := &broker{
		opts: opts,
	}

	chainPublisherInterceptors(c)
	chainSubscriberInterceptors(c)

	return c
}

// broker acts as a wrapper of another broker with interception capabilities.
type broker struct {
	opts options
}

func (c *broker) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	if c.opts.publishInterceptor == nil {
		return c.opts.provider.Publish(ctx, topic, m)
	}

	mw := c.opts.publishInterceptor(ctx, func(ctx context.Context, m interface{}, topic pubsub.Topic) error {
		return c.opts.provider.Publish(ctx, topic, m)
	})

	return mw(ctx, m, topic)
}

func (c *broker) Subscribe(ctx context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	// no interceptor, then just deliver
	if c.opts.subscribeInterceptor == nil {
		return c.opts.provider.Subscribe(ctx, topic, subscriber)
	}

	mw := c.opts.subscribeInterceptor(ctx, func(ctx context.Context, s *pubsub.Subscriber, m interface{}) error {
		return nil
	})

	// black magic: access to private property holding subscriber's handler function
	rs := reflect.ValueOf(subscriber).Elem()
	rf := rs.FieldByName("handlerFunc")
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	originalCallable := rf.Interface().(reflect.Value)

	newCallable := reflect.ValueOf(func(ctx context.Context, m interface{}) error {
		// apply interceptors upon reception
		if err := mw(ctx, subscriber, m); err != nil {
			return err
		}

		// pass to the origin handling function
		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(m),
		}

		if out := originalCallable.Call(args); out[0].Interface() != nil {
			return out[0].Interface().(error)
		}

		return nil
	})

	rf.Set(reflect.ValueOf(newCallable))

	return c.opts.provider.Subscribe(ctx, topic, subscriber)
}

func (c *broker) Unsubscribe(ctx context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	return c.opts.provider.Unsubscribe(ctx, topic, subscriber)
}

func (c *broker) Subscriptions(ctx context.Context) (map[pubsub.Topic][]*pubsub.Subscriber, error) {
	return c.opts.provider.Subscriptions(ctx)
}

func (c *broker) Shutdown(ctx context.Context) error {
	return c.opts.provider.Shutdown(ctx)
}
