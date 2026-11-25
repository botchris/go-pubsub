package pool

import (
	"context"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/botchris/go-pubsub"
)

type middleware struct {
	pubsub.Broker
	options *options
	pool    pond.Pool
}

// NewPoolMiddleware returns a middleware that delivers messages concurrently and asynchronously to
// the provided Broker.
func NewPoolMiddleware(broker pubsub.Broker, maxConcurrency int, o ...Option) pubsub.Broker {
	opts := &options{
		timeout:     5 * time.Second,
		queueSize:   0,
		nonBlocking: false,
		onError:     func(t pubsub.Topic, m interface{}, err error) {},
	}

	for _, opt := range o {
		opt(opts)
	}

	return &middleware{
		Broker:  broker,
		options: opts,
		pool: pond.NewPool(
			maxConcurrency,
			pond.WithQueueSize(opts.queueSize),
			pond.WithNonBlocking(opts.nonBlocking),
		),
	}
}

func (mw middleware) Publish(_ context.Context, topic pubsub.Topic, m interface{}) error {
	mw.pool.Submit(func() {
		ctx, cancel := context.WithTimeout(context.Background(), mw.options.timeout)
		defer cancel()

		if pErr := mw.Broker.Publish(ctx, topic, m); pErr != nil {
			mw.options.onError(topic, m, pErr)
		}
	})

	return nil
}

func (mw middleware) Shutdown(ctx context.Context) error {
	mw.pool.StopAndWait()

	return mw.Broker.Shutdown(ctx)
}
