package pool

import "time"

// Option is a configuration function that can be passed to NewPoolMiddleware.
type Option func(*options)

type options struct {
	timeout     time.Duration
	queueSize   int
	nonBlocking bool
	onError     func(m interface{}, err error)
}

// WithOnError sets Publish error handler function.
func WithOnError(onError func(m interface{}, err error)) Option {
	return func(p *options) {
		p.onError = onError
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
func WithQueueSize(size int) Option {
	return func(p *options) {
		p.queueSize = size
	}
}

// WithNonBlocking sets the pool to be non-blocking when the queue is full.
// This option is only effective when the queue size is set.
func WithNonBlocking(nonBlocking bool) Option {
	return func(p *options) {
		p.nonBlocking = nonBlocking
	}
}
