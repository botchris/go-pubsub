package retry

import "github.com/botchris/go-pubsub"

type options struct {
	publishStrategy Strategy
	deliverStrategy Strategy
	publishError    PublishErrorHandler
}

// PublishErrorHandler is a function that handles errors that occur during the
// publish operation after all retries have been exhausted.
type PublishErrorHandler func(t pubsub.Topic, m interface{}, err error)

// Option is a configuration function that can be passed to NewRetryMiddleware.
type Option func(*options)

// WithPublishStrategy sets the strategy to be used when retrying a publish operation.
func WithPublishStrategy(s Strategy) Option {
	return func(o *options) {
		o.publishStrategy = s
	}
}

// WithDeliverStrategy sets the strategy to be used when retrying a deliver operation
// to a subscriber handler function.
func WithDeliverStrategy(s Strategy) Option {
	return func(o *options) {
		o.deliverStrategy = s
	}
}

// WithPublishError sets the error handler function to be called when a publish operation
// fails after all retries have been exhausted.
func WithPublishError(fn PublishErrorHandler) Option {
	return func(o *options) {
		o.publishError = fn
	}
}
