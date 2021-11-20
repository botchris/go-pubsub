package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/botchris/go-pubsub"
)

type middleware struct {
	pubsub.Broker
	publishStrategy    Strategy
	subscriberStrategy Strategy
}

// NewRetryMiddleware returns a middleware that retries messages that fail to
// be published to topics or delivered to subscribers.
func NewRetryMiddleware(broker pubsub.Broker, p Strategy, s Strategy) pubsub.Broker {
	return &middleware{
		Broker:             broker,
		publishStrategy:    p,
		subscriberStrategy: s,
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

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, sub pubsub.Subscriber) error {
	s := &subscriber{
		Subscriber: sub,
		strategy:   mw.subscriberStrategy,
	}

	return mw.Broker.Subscribe(ctx, topic, s)
}
