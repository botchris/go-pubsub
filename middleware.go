package pubsub

import (
	"context"
	"reflect"
)

// PublishHandler wraps a call to publish, for interception
type PublishHandler func(ctx context.Context, m interface{}, topic Topic) error

// SubscribeMessageHandler defines the handler invoked by SubscriberInterceptor before a message is delivered to a particular subscriber.
type SubscribeMessageHandler func(ctx context.Context, s *Subscriber, m interface{}) error

// PublishInterceptor provides a hook to intercept each message before it gets published.
type PublishInterceptor func(ctx context.Context, next PublishHandler) PublishHandler

// SubscriberInterceptor provides a hook to intercept each message before it gets delivered to subscribers.
//
// Please note that interception occurs for each delivery operation.
// For instance, if two subscribers `S1` and `S2` receives the same message `M`, then interception logic will be
// triggered twice for the same message `M`.
type SubscriberInterceptor func(ctx context.Context, next SubscribeMessageHandler) SubscribeMessageHandler

// Option defines an option for a broker middleware.
type Option interface {
	apply(*options)
}

type options struct {
	provider                   Broker
	publishInterceptor         PublishInterceptor
	chainPublishInterceptors   []PublishInterceptor
	subscribeInterceptor       SubscriberInterceptor
	chainSubscribeInterceptors []SubscriberInterceptor
}

// funcOption wraps a function that modifies options into an implementation of the Option interface.
type funcOption struct {
	f func(*options)
}

func (fdo *funcOption) apply(do *options) {
	fdo.f(do)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

// WithPublishInterceptor returns an Option that sets the PublishInterceptor middleware for the broker.
// Only one interceptor can be installed. The construction of multiple interceptors (e.g., chaining)
// can be implemented at the caller.
func WithPublishInterceptor(i PublishInterceptor) Option {
	return newFuncOption(func(o *options) {
		if o.publishInterceptor != nil {
			panic("the middleware publish interceptor was already set and may not be reset.")
		}

		o.publishInterceptor = i
	})
}

// WithChainPublishInterceptors returns an Option that specifies the chained interceptor for publishing.
// The first interceptor will be the outermost, while the last interceptor will be the innermost wrapper
// around the real call. All publish interceptors added by this method will be chained.
func WithChainPublishInterceptors(interceptors ...PublishInterceptor) Option {
	return newFuncOption(func(o *options) {
		o.chainPublishInterceptors = append(o.chainPublishInterceptors, interceptors...)
	})
}

// WithSubscribeInterceptor returns an Option that sets the SubscriberInterceptor for the broker.
// Only one interceptor can be installed. The construction of multiple interceptors (e.g., chaining)
// can be implemented at the caller.
func WithSubscribeInterceptor(i SubscriberInterceptor) Option {
	return newFuncOption(func(o *options) {
		if o.subscribeInterceptor != nil {
			panic("the middleware subscribe interceptor was already set and may not be reset.")
		}

		o.subscribeInterceptor = i
	})
}

// WithChainSubscribeInterceptors returns an Option that specifies the chained interceptor for subscribing.
// The first interceptor will be the outermost, while the last interceptor will be the innermost wrapper around the
// real call. All subscribe interceptors added by this method will be chained.
func WithChainSubscribeInterceptors(interceptors ...SubscriberInterceptor) Option {
	return newFuncOption(func(o *options) {
		o.chainSubscribeInterceptors = append(o.chainSubscribeInterceptors, interceptors...)
	})
}

func chainPublisherInterceptors(c *middleware) {
	// Prepend opts.publishInterceptor to the chaining interceptors if it exists, since publishInterceptor will
	// be executed before any other chained interceptors.
	interceptors := c.opts.chainPublishInterceptors
	if c.opts.publishInterceptor != nil {
		interceptors = append([]PublishInterceptor{c.opts.publishInterceptor}, c.opts.chainPublishInterceptors...)
	}

	var chainedInt PublishInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, next PublishHandler) PublishHandler {
			return func(ctx context.Context, m interface{}, topic Topic) error {
				return interceptors[0](ctx, getChainPublishHandler(interceptors, 0, next))(ctx, m, topic)
			}
		}
	}

	c.opts.publishInterceptor = chainedInt
}

// getChainPublishHandler recursively generate the chained PublishHandler
func getChainPublishHandler(interceptors []PublishInterceptor, curr int, final PublishHandler) PublishHandler {
	if curr == len(interceptors)-1 {
		return final
	}

	return func(ctx context.Context, m interface{}, topic Topic) error {
		return interceptors[curr+1](ctx, getChainPublishHandler(interceptors, curr+1, final))(ctx, m, topic)
	}
}

func chainSubscriberInterceptors(c *middleware) {
	// Prepend opts.publishInterceptor to the chaining interceptors if it exists, since publishInterceptor will
	// be executed before any other chained interceptors.
	interceptors := c.opts.chainSubscribeInterceptors
	if c.opts.subscribeInterceptor != nil {
		interceptors = append([]SubscriberInterceptor{c.opts.subscribeInterceptor}, c.opts.chainSubscribeInterceptors...)
	}

	var chainedInt SubscriberInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, next SubscribeMessageHandler) SubscribeMessageHandler {
			return func(ctx context.Context, s *Subscriber, m interface{}) error {
				return interceptors[0](ctx, getChainSubscribeHandler(interceptors, 0, next))(ctx, s, m)
			}
		}
	}

	c.opts.subscribeInterceptor = chainedInt
}

// getChainPublishHandler recursively generate the chained PublishHandler
func getChainSubscribeHandler(interceptors []SubscriberInterceptor, curr int, final SubscribeMessageHandler) SubscribeMessageHandler {
	if curr == len(interceptors)-1 {
		return final
	}

	return func(ctx context.Context, s *Subscriber, m interface{}) error {
		return interceptors[curr+1](ctx, getChainSubscribeHandler(interceptors, curr+1, final))(ctx, s, m)
	}
}

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

	mw := md.opts.publishInterceptor(ctx, func(ctx context.Context, m interface{}, topic Topic) error {
		return md.opts.provider.Publish(ctx, topic, m)
	})

	return mw(ctx, m, topic)
}

func (md *middleware) Subscribe(ctx context.Context, topic Topic, subscriber *Subscriber) error {
	// no interceptor, then just deliver
	if md.opts.subscribeInterceptor == nil {
		return md.opts.provider.Subscribe(ctx, topic, subscriber)
	}

	mw := md.opts.subscribeInterceptor(ctx, func(ctx context.Context, s *Subscriber, m interface{}) error {
		return nil
	})

	originalCallable := subscriber.handlerFunc
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
