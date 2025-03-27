package pool

import (
	"time"

	"github.com/botchris/go-pubsub"
)

// Option is a configuration function that can be passed to NewPoolMiddleware.
type Option func(*options)

// PublishErrorHandler is a function that handles errors that occur during the
// publish operation.
type PublishErrorHandler func(t pubsub.Topic, m interface{}, err error)

type options struct {
	timeout     time.Duration
	queueSize   int
	nonBlocking bool
	onError     PublishErrorHandler
}

// WithOnError sets Publish error handler function.
func WithOnError(fn PublishErrorHandler) Option {
	return func(p *options) {
		p.onError = fn
	}
}

// WithTimeout sets the timeout for the publish operation which is executed in a
// separate goroutine.
func WithTimeout(timeout time.Duration) Option {
	return func(p *options) {
		p.timeout = timeout
	}
}

// WithQueueSize sets the max number of elements that can be queued in the pool.
//
// By default, messages are queued indefinitely until the pool is stopped (or the process
// runs out of memory). You can limit the number of messages that can be queued by setting
// a queue size option.
func WithQueueSize(size int) Option {
	return func(p *options) {
		p.queueSize = size
	}
}

// WithNonBlocking sets the pool to be non-blocking when the queue is full.
// This option is only effective when the queue size is set.
//
// By default, when using queue size option, messages publishing blocks until there is space in the queue,
// but you can change this behavior to non-blocking by setting the WithNonBlocking option to true.
// If the queue is full and non-blocking mode is enabled, the message is dropped and an error is returned
// (pond.ErrQueueFull).
func WithNonBlocking(nonBlocking bool) Option {
	return func(p *options) {
		p.nonBlocking = nonBlocking
	}
}
