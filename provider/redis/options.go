package redis

import "time"

// Option represents an option that can be passed to redis broker.
type Option func(*options)

type options struct {
	address                string
	logger                 Logger
	deliverTimeout         time.Duration
	readGroupTimeout       time.Duration
	pendingIdleTime        time.Duration
	janitorConsumerTimeout time.Duration
	janitorFrequency       time.Duration
	trimDuration           time.Duration
}

// WithAddress sets the address of the redis server.
func WithAddress(address string) Option {
	return func(o *options) {
		o.address = address
	}
}

// WithLogger defines a logger for the broker. Default: noop logger.
func WithLogger(logger Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}

// WithDeliverTimeout how long to wait trying to send a message to a handler
// until we consider it has timed out. Default: 10s
func WithDeliverTimeout(to time.Duration) Option {
	return func(opts *options) {
		opts.deliverTimeout = to
	}
}

// WithReadGroupTimeout how long to block on call to redis. Default: 10s
func WithReadGroupTimeout(readGroupTimeout time.Duration) Option {
	return func(opts *options) {
		opts.readGroupTimeout = readGroupTimeout
	}
}

// WithPendingIdleTime how long in pending before we claim a message from a
// different consumer. Default: 1 minute
func WithPendingIdleTime(pendingIdleTime time.Duration) Option {
	return func(opts *options) {
		opts.pendingIdleTime = pendingIdleTime
	}
}

// WithJanitorConsumerTimeout threshold for an "old" consumer. Default: 1 day
func WithJanitorConsumerTimeout(janitorConsumerTimeout time.Duration) Option {
	return func(opts *options) {
		opts.janitorConsumerTimeout = janitorConsumerTimeout
	}
}

// WithJanitorFrequency how often do we run the janitor. Default: 4 hour
func WithJanitorFrequency(janitorFrequency time.Duration) Option {
	return func(opts *options) {
		opts.janitorFrequency = janitorFrequency
	}
}

// WithTrimDuration oldest event in stream. Default: 5 days
func WithTrimDuration(trimDuration time.Duration) Option {
	return func(opts *options) {
		opts.trimDuration = trimDuration
	}
}
