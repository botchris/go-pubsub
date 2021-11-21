package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/botchris/go-pubsub"
)

type middleware struct {
	pubsub.Broker
	publishStrategy Strategy
	deliverStrategy Strategy
}

// NewRetryMiddleware returns a middleware that retries messages that fail to
// be published to topics or delivered to subscribers.
//
// Strategies must be configured accordingly, so they can retry as long as
// operation contexts keeps alive. For instance, when publishing a message, the
// maximum execution time is determined by the provided context.
func NewRetryMiddleware(broker pubsub.Broker, publish Strategy, deliver Strategy) pubsub.Broker {
	return &middleware{
		Broker:          broker,
		publishStrategy: publish,
		deliverStrategy: deliver,
	}
}

func (mw middleware) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	done := ctx.Done()

retry:
	select {
	case <-done:
		return fmt.Errorf("context cancelled")
	default:
	}

	if backoff := mw.publishStrategy.Proceed(topic, m); backoff > 0 {
		select {
		case <-time.After(backoff):
			// TODO: This branch holds up the next try. Before, we
			// would simply break to the "retry" label and then possibly wait
			// again. However, this requires all retry strategies to have a
			// large probability of probing the sync for success, rather than
			// just backing off and sending the request.
		case <-done:
			return fmt.Errorf("context cancelled")
		}
	}

	if nErr := mw.Broker.Publish(ctx, topic, m); nErr != nil {
		if mw.publishStrategy.Failure(topic, m, nErr) {
			fmt.Printf("retrying publish error, cause: message was dropped, retries exhausted {topic=%s, error=%s}\n", topic, nErr)

			return nil
		}

		goto retry
	}

	mw.publishStrategy.Success(topic, m)

	return nil
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, sub pubsub.Subscriber, option ...pubsub.SubscribeOption) error {
	s := &subscriber{
		Subscriber: sub,
		strategy:   mw.deliverStrategy,
	}

	return mw.Broker.Subscribe(ctx, topic, s, option...)
}
