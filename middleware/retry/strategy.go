// Package retry provides retry strategies for retry middleware.
// Credits: https://github.com/docker/go-events
package retry

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/botchris/go-pubsub"
)

// Strategy defines a strategy for retrying publish or delivery operations.
//
// All methods should be goroutine safe.
type Strategy interface {
	// Proceed is called before every message is published or delivered to
	// handler. If proceed returns a positive, non-zero integer, the retryer
	// will back off by the provided duration.
	//
	// A message and a topic are provided, they may be ignored.
	Proceed(topic pubsub.Topic, msg interface{}) time.Duration

	// Failure reports a failure to the strategy. If this method returns true,
	// the message should be dropped.
	Failure(topic pubsub.Topic, msg interface{}, err error) bool

	// Success should be called when a message is successfully sent.
	Success(topic pubsub.Topic, msg interface{})
}

// Breaker implements a circuit breaker retry strategy.
//
// The current implementation never drops messages.
type breakerStrategy struct {
	threshold int
	recent    int
	last      time.Time
	backoff   time.Duration // time after which we retry after failure.
	mu        sync.Mutex
}

// NewBreakerStrategy returns a breaker that will backoff after the threshold
// has been tripped. A Breaker is thread safe and may be shared by many
// goroutines.
func NewBreakerStrategy(threshold int, backoff time.Duration) Strategy {
	return &breakerStrategy{
		threshold: threshold,
		backoff:   backoff,
	}
}

// Proceed checks the failures against the threshold.
func (b *breakerStrategy) Proceed(_ pubsub.Topic, _ interface{}) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.recent < b.threshold {
		return 0
	}

	return time.Until(b.last.Add(b.backoff))
}

// Success resets the breaker.
func (b *breakerStrategy) Success(_ pubsub.Topic, _ interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.recent = 0
	b.last = time.Time{}
}

// Failure records the failure and latest failure time.
func (b *breakerStrategy) Failure(_ pubsub.Topic, _ interface{}, _ error) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.recent++
	b.last = time.Now().UTC()

	return false // never drop messages.
}

// ExponentialBackoffConfig configures backoff parameters.
//
// Note that these parameters operate on the upper bound for choosing a random
// value. For example, at Base=1s, a random value in [0,1s) will be chosen for
// the backoff value.
type ExponentialBackoffConfig struct {
	// Base is the minimum bound for backing off after failure.
	Base time.Duration

	// Factor sets the amount of time by which the backoff grows with each
	// failure.
	Factor time.Duration

	// Max is the absolute maximum bound for a single backoff.
	Max time.Duration
}

// DefaultExponentialBackoffConfig provides a default configuration for
// exponential backoff.
var DefaultExponentialBackoffConfig = ExponentialBackoffConfig{
	Base:   time.Second,
	Factor: time.Second,
	Max:    20 * time.Second,
}

// NewExponentialBackoff returns an exponential backoff strategy with the
// desired config. If config is nil, the default is returned.
func NewExponentialBackoff(config ExponentialBackoffConfig) Strategy {
	return &exponentialBackoffStrategy{
		config: config,
	}
}

// exponentialBackoffStrategy implements random backoff with exponentially
// increasing bounds as the number consecutive failures increase.
type exponentialBackoffStrategy struct {
	failures uint64 // consecutive failure counter (needs to be 64-bit aligned)
	config   ExponentialBackoffConfig
}

// Proceed returns the next randomly bound exponential backoff time.
func (b *exponentialBackoffStrategy) Proceed(_ pubsub.Topic, _ interface{}) time.Duration {
	return b.backoff(atomic.LoadUint64(&b.failures))
}

// Success resets the failures counter.
func (b *exponentialBackoffStrategy) Success(_ pubsub.Topic, _ interface{}) {
	atomic.StoreUint64(&b.failures, 0)
}

// Failure increments the failure counter.
func (b *exponentialBackoffStrategy) Failure(_ pubsub.Topic, _ interface{}, _ error) bool {
	atomic.AddUint64(&b.failures, 1)

	return false
}

// backoff calculates the amount of time to wait based on the number of
// consecutive failures.
func (b *exponentialBackoffStrategy) backoff(failures uint64) time.Duration {
	if failures <= 0 {
		// proceed normally when there are no failures.
		return 0
	}

	factor := b.config.Factor
	if factor <= 0 {
		factor = DefaultExponentialBackoffConfig.Factor
	}

	backoff := b.config.Base + factor*time.Duration(1<<(failures-1))

	max := b.config.Max
	if max <= 0 {
		max = DefaultExponentialBackoffConfig.Max
	}

	if backoff > max || backoff < 0 {
		backoff = max
	}

	// Choose a uniformly distributed value from [0, backoff).
	return time.Duration(rand.Int63n(int64(backoff)))
}
