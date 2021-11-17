package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/botchris/go-pubsub"
)

// PublishInterceptor adds panic recovery capabilities to publishers.
func PublishInterceptor(strategy Strategy) pubsub.PublishInterceptor {
	return func(_ context.Context, next pubsub.PublishHandler) pubsub.PublishHandler {
		return func(_ context.Context, topic pubsub.Topic, m interface{}) (err error) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := ctx.Done()

		retry:
			select {
			case <-done:
				return fmt.Errorf("context cancelled")
			default:
			}

			if backoff := strategy.Proceed(topic, m); backoff > 0 {
				select {
				case <-time.After(backoff):
					// TODO(stevvooe): This branch holds up the next try. Before, we
					// would simply break to the "retry" label and then possibly wait
					// again. However, this requires all retry strategies to have a
					// large probability of probing the sync for success, rather than
					// just backing off and sending the request.
				case <-done:
					return fmt.Errorf("context cancelled")
				}
			}

			if nErr := next(ctx, topic, m); nErr != nil {
				if strategy.Failure(topic, m, nErr) {
					fmt.Printf("retrying publish error, cause: message was dropped, retries exhausted {topic=%s, error=%s}\n", topic, nErr)

					return nil
				}

				goto retry
			}

			strategy.Success(topic, m)

			return nil
		}
	}
}

// SubscriberInterceptor adds panic recovery capabilities to subscribers.
func SubscriberInterceptor(strategy Strategy) pubsub.SubscriberInterceptor {
	return func(_ context.Context, next pubsub.SubscriberMessageHandler) pubsub.SubscriberMessageHandler {
		return func(_ context.Context, s *pubsub.Subscriber, topic pubsub.Topic, m interface{}) (err error) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := ctx.Done()

		retry:
			select {
			case <-done:
				return fmt.Errorf("context cancelled")
			default:
			}

			if backoff := strategy.Proceed(topic, m); backoff > 0 {
				select {
				case <-time.After(backoff):
					// TODO(stevvooe): This branch holds up the next try. Before, we
					// would simply break to the "retry" label and then possibly wait
					// again. However, this requires all retry strategies to have a
					// large probability of probing the sync for success, rather than
					// just backing off and sending the request.
				case <-done:
					return fmt.Errorf("context cancelled")
				}
			}

			if nErr := next(ctx, s, topic, m); nErr != nil {
				if strategy.Failure(topic, m, nErr) {
					fmt.Printf("retrying delivery error, cause: message was dropped, retries exhausted {topic=%s, error=%s}\n", topic, nErr)

					return nil
				}

				goto retry
			}

			strategy.Success(topic, m)

			return nil
		}
	}
}
