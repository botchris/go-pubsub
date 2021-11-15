package recover

import (
	"context"
	"fmt"

	"github.com/botchris/go-pubsub"
)

// RecoveryHandlerFunc is a function that recovers from the panic `p` by returning an `error`.
type RecoveryHandlerFunc func(ctx context.Context, p interface{}) error

// PublishInterceptor adds panic recovery capabilities to publishers.
func PublishInterceptor(fn RecoveryHandlerFunc) pubsub.PublishInterceptor {
	return func(ctx context.Context, next pubsub.PublishHandler) pubsub.PublishHandler {
		return func(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
			defer func(ctx context.Context) {
				if r := recover(); r != nil {
					err = recoverFrom(ctx, r, "pubsub: publisher panic\n", fn)
				}
			}(ctx)

			err = next(ctx, topic, m)

			return
		}
	}
}

// SubscriberInterceptor adds panic recovery capabilities to subscribers.
func SubscriberInterceptor(fn RecoveryHandlerFunc) pubsub.SubscriberInterceptor {
	return func(ctx context.Context, next pubsub.SubscriberMessageHandler) pubsub.SubscriberMessageHandler {
		return func(ctx context.Context, s *pubsub.Subscriber, t pubsub.Topic, m interface{}) (err error) {
			defer func(ctx context.Context) {
				if r := recover(); r != nil {
					err = recoverFrom(ctx, r, "pubsub: subscriber panic\n", fn)
				}
			}(ctx)

			err = next(ctx, s, t, m)

			return
		}
	}
}

func recoverFrom(ctx context.Context, p interface{}, wrap string, r RecoveryHandlerFunc) error {
	if r == nil {
		return fmt.Errorf("%s: %s", p, wrap)
	}

	return r(ctx, p)
}
