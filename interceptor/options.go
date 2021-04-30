package interceptor

import (
	"context"

	"github.com/ChristopherCastro/go-pubsub"
)

// Option sets options for a interceptor broker instance.
type Option interface {
	apply(*options)
}

type options struct {
	provider                   pubsub.Broker
	publishInterceptor         PublishInterceptor
	chainPublishInterceptors   []PublishInterceptor
	subscribeInterceptor       SubscribeInterceptor
	chainSubscribeInterceptors []SubscribeInterceptor
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

// WithPublishInterceptor returns a Option that sets the PublishInterceptor middleware for the broker.
// Only one interceptor can be installed. The construction of multiple interceptors (e.g., chaining)
// can be implemented at the caller.
func WithPublishInterceptor(i PublishInterceptor) Option {
	return newFuncOption(func(o *options) {
		if o.publishInterceptor != nil {
			panic("the broker publish interceptor was already set and may not be reset.")
		}

		o.publishInterceptor = i
	})
}

// WithChainPublishInterceptors returns a Option that specifies the chained interceptor
// for publishing. The first interceptor will be the outer most,
// while the last interceptor will be the inner most wrapper around the real call.
// All publish interceptors added by this method will be chained.
func WithChainPublishInterceptors(interceptors ...PublishInterceptor) Option {
	return newFuncOption(func(o *options) {
		o.chainPublishInterceptors = append(o.chainPublishInterceptors, interceptors...)
	})
}

// WithSubscribeInterceptor returns a Option that sets the SubscribeInterceptor for the broker.
// Only one interceptor can be installed. The construction of multiple interceptors (e.g., chaining)
// can be implemented at the caller.
func WithSubscribeInterceptor(i SubscribeInterceptor) Option {
	return newFuncOption(func(o *options) {
		if o.subscribeInterceptor != nil {
			panic("the broker subscribe interceptor was already set and may not be reset.")
		}

		o.subscribeInterceptor = i
	})
}

// WithChainSubscribeInterceptors returns a Option that specifies the chained interceptor
// for subscribing. The first interceptor will be the outer most,
// while the last interceptor will be the inner most wrapper around the real call.
// All subscribe interceptors added by this method will be chained.
func WithChainSubscribeInterceptors(interceptors ...SubscribeInterceptor) Option {
	return newFuncOption(func(o *options) {
		o.chainSubscribeInterceptors = append(o.chainSubscribeInterceptors, interceptors...)
	})
}

func chainPublisherInterceptors(c *broker) {
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
			return func(ctx context.Context, m interface{}, topic pubsub.Topic) error {
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

	return func(ctx context.Context, m interface{}, topic pubsub.Topic) error {
		return interceptors[curr+1](ctx, getChainPublishHandler(interceptors, curr+1, final))(ctx, m, topic)
	}
}

func chainSubscriberInterceptors(c *broker) {
	// Prepend opts.publishInterceptor to the chaining interceptors if it exists, since publishInterceptor will
	// be executed before any other chained interceptors.
	interceptors := c.opts.chainSubscribeInterceptors
	if c.opts.subscribeInterceptor != nil {
		interceptors = append([]SubscribeInterceptor{c.opts.subscribeInterceptor}, c.opts.chainSubscribeInterceptors...)
	}

	var chainedInt SubscribeInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, next SubscribeMessageHandler) SubscribeMessageHandler {
			return func(ctx context.Context, s *pubsub.Subscriber, m interface{}) error {
				return interceptors[0](ctx, getChainSubscribeHandler(interceptors, 0, next))(ctx, s, m)
			}
		}
	}

	c.opts.subscribeInterceptor = chainedInt
}

// getChainPublishHandler recursively generate the chained PublishHandler
func getChainSubscribeHandler(interceptors []SubscribeInterceptor, curr int, final SubscribeMessageHandler) SubscribeMessageHandler {
	if curr == len(interceptors)-1 {
		return final
	}

	return func(ctx context.Context, s *pubsub.Subscriber, m interface{}) error {
		return interceptors[curr+1](ctx, getChainSubscribeHandler(interceptors, curr+1, final))(ctx, s, m)
	}
}
