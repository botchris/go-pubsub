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
	options         *options
}

// NewRetryMiddleware returns a middleware that retries messages that fail to
// be published to topics or delivered to handlers.
//
// Strategies must be configured accordingly, so they can retry as long as
// operation contexts keeps alive. For instance, when publishing a message, the
// maximum execution time is determined by the provided context. A nil strategy
// defaults to an exponential backoff with DefaultExponentialBackoffConfig.
func NewRetryMiddleware(broker pubsub.Broker, publish Strategy, deliver Strategy, o ...Option) pubsub.Broker {
	if publish == nil {
		publish = NewExponentialBackoff(DefaultExponentialBackoffConfig)
	}

	if deliver == nil {
		deliver = NewExponentialBackoff(DefaultExponentialBackoffConfig)
	}

	opts := &options{
		publishError: func(t pubsub.Topic, m interface{}, err error) {
			fmt.Printf("retrying publish error, cause: message was dropped, retries exhausted {topic=%s, error=%s}\n", t, err)
		},
	}

	for _, opt := range o {
		opt(opts)
	}

	return &middleware{
		Broker:          broker,
		publishStrategy: publish,
		deliverStrategy: deliver,
		options:         opts,
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
			mw.options.publishError(topic, m, nErr)

			return nil
		}

		goto retry
	}

	mw.publishStrategy.Success(topic, m)

	return nil
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, h pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	s := &handler{
		Handler:  h,
		strategy: mw.deliverStrategy,
	}

	return mw.Broker.Subscribe(ctx, topic, s, option...)
}
