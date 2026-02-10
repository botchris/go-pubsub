package pool

import (
	"context"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/botchris/go-pubsub"
)

const metadataCtxKey = iota

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
		onError:     func(m interface{}, err error) {},
	}

	for i := range o {
		o[i](opts)
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

func (mw middleware) Publish(pubCtx context.Context, topic pubsub.Topic, m interface{}) error {
	var ctxMD map[string]any
	if mw.options.ctxMetadataFunc != nil {
		ctxMD = mw.options.ctxMetadataFunc(pubCtx)
	}

	mw.pool.Submit(func() {
		poolCtx, cancel := context.WithTimeout(context.Background(), mw.options.timeout)
		defer cancel()

		if ctxMD != nil {
			poolCtx = context.WithValue(poolCtx, metadataCtxKey, ctxMD)
		}

		if pErr := mw.Broker.Publish(poolCtx, topic, m); pErr != nil {
			mw.options.onError(m, pErr)
		}
	})

	return nil
}

func (mw middleware) Shutdown(ctx context.Context) error {
	mw.pool.StopAndWait()

	return mw.Broker.Shutdown(ctx)
}

// MetadataFromContext retrieves the metadata piped from the Publish context in the pool goroutine context.
func MetadataFromContext(ctx context.Context) map[string]any {
	md, exists := ctx.Value(metadataCtxKey).(map[string]any)
	if !exists {
		return make(map[string]any)
	}

	return md
}
