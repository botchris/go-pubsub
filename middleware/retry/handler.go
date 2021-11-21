package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/botchris/go-pubsub"
)

type handler struct {
	pubsub.Handler
	strategy Strategy
}

func (s *handler) Deliver(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	done := ctx.Done()

retry:
	select {
	case <-done:
		return fmt.Errorf("context cancelled")
	default:
	}

	if backoff := s.strategy.Proceed(topic, m); backoff > 0 {
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

	if err := s.Handler.Deliver(ctx, topic, m); err != nil {
		if s.strategy.Failure(topic, m, err) {
			fmt.Printf("retrying delivery error, cause: message was dropped, retries exhausted {topic=%s, error=%s}\n", topic, err)

			return nil
		}

		goto retry
	}

	s.strategy.Success(topic, m)

	return nil
}
