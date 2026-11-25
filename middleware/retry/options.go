package retry

import "github.com/botchris/go-pubsub"

type options struct {
	publishError PublishErrorHandler
}

// PublishErrorHandler is a function that handles errors that occur during the
// publish operation after all retries have been exhausted.
type PublishErrorHandler func(t pubsub.Topic, m interface{}, err error)

// Option is a configuration function that can be passed to NewRetryMiddleware.
type Option func(*options)

// WithPublishError sets the error handler function to be called when a publish operation
// fails after all retries have been exhausted.
func WithPublishError(fn PublishErrorHandler) Option {
	return func(o *options) {
		o.publishError = fn
	}
}
