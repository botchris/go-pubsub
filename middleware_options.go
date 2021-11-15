package pubsub

import "context"

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
			return func(ctx context.Context, topic Topic, m interface{}) error {
				return interceptors[0](ctx, getChainPublishHandler(interceptors, 0, next))(ctx, topic, m)
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

	return func(ctx context.Context, topic Topic, m interface{}) error {
		return interceptors[curr+1](ctx, getChainPublishHandler(interceptors, curr+1, final))(ctx, topic, m)
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
			return func(ctx context.Context, s *Subscriber, t Topic, m interface{}) error {
				return interceptors[0](ctx, getChainSubscribeHandler(interceptors, 0, next))(ctx, s, t, m)
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

	return func(ctx context.Context, s *Subscriber, t Topic, m interface{}) error {
		return interceptors[curr+1](ctx, getChainSubscribeHandler(interceptors, curr+1, final))(ctx, s, t, m)
	}
}
