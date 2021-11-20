package redis

import "time"

// Option represents an option that can be passed to redis broker.
type Option func(*options)

type options struct {
	groupID                string
	logger                 Logger
	consumerTimeout        time.Duration
	readGroupTimeout       time.Duration
	pendingIdleTime        time.Duration
	janitorConsumerTimeout time.Duration
	janitorFrequency       time.Duration
	trimDuration           time.Duration
}

// WithGroupID defines a group ID for the broker. Brokers with the same group ID
// shares the same message queue. Default: random uuid
func WithGroupID(groupID string) Option {
	return func(opts *options) {
		opts.groupID = groupID
	}
}

// WithLogger defines a logger for the broker. Default: noop logger.
func WithLogger(logger Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}

// WithConsumerTimeout how long to wait trying to send event to a consumer's
// channel until we consider it has timed out. Default: 10s
func WithConsumerTimeout(consumerTimeout time.Duration) Option {
	return func(opts *options) {
		opts.consumerTimeout = consumerTimeout
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
