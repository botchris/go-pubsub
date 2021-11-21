package retry_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/retry"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
)

func TestPublishInterceptor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("GIVEN a broker that works once every 5 publishes and with an exponential backoff recovery middleware", func(t *testing.T) {
		mock := &intermittentFailBroker{worksEvery: 5}
		broker := pubsub.Broker(mock)

		strategy := retry.NewExponentialBackoff(retry.ExponentialBackoffConfig{
			Base:   100 * time.Millisecond,
			Factor: 100 * time.Millisecond,
			Max:    5 * time.Second,
		})
		broker = retry.NewRetryMiddleware(broker, strategy, strategy)

		t.Run("WHEN underlying publishing fails", func(t *testing.T) {
			err := broker.Publish(ctx, "topic", "message")

			t.Run("THEN publish eventually succeeds", func(t *testing.T) {
				require.NoError(t, err)

				mock.mu.RLock()
				calls := mock.calls
				mock.mu.RUnlock()

				require.GreaterOrEqual(t, calls, 5)
			})
		})
	})

	t.Run("GIVEN a broker that works once every 5 publishes and with an breaker recovery middleware", func(t *testing.T) {
		mock := &intermittentFailBroker{worksEvery: 5}
		broker := pubsub.Broker(mock)

		strategy := retry.NewBreakerStrategy(5, time.Second)
		broker = retry.NewRetryMiddleware(broker, strategy, strategy)

		t.Run("WHEN underlying publishing fails 5 times in a row", func(t *testing.T) {
			err := broker.Publish(ctx, "topic", "message")

			t.Run("THEN publish eventually succeeds after waiting 1 second", func(t *testing.T) {
				require.NoError(t, err)

				mock.mu.RLock()
				calls := mock.calls
				mock.mu.RUnlock()

				require.GreaterOrEqual(t, calls, 5)
			})
		})
	})
}

func TestSubscriberInterceptor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("GIVEN memory broker with an exponential backoff delivery and a subscriber that works once every 5 calls", func(t *testing.T) {
		calls := 0
		worksEvery := 5
		handler := func(_ context.Context, t pubsub.Topic, m string) error {
			calls++

			if calls%worksEvery == 0 {
				return nil
			}

			return fmt.Errorf("fails")
		}

		strategy := retry.NewExponentialBackoff(retry.ExponentialBackoffConfig{
			Base:   100 * time.Millisecond,
			Factor: 100 * time.Millisecond,
			Max:    5 * time.Second,
		})

		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = retry.NewRetryMiddleware(broker, strategy, strategy)
		topic := pubsub.Topic("test")
		sub := pubsub.NewHandler(handler)

		require.NoError(t, broker.Subscribe(ctx, topic, sub))

		t.Run("WHEN publishing a message to such subscriber", func(t *testing.T) {
			err := broker.Publish(ctx, topic, "message")

			t.Run("THEN subscriber eventually receives the message after 5 attempts", func(t *testing.T) {
				require.NoError(t, err)
				require.GreaterOrEqual(t, calls, 5)
			})
		})
	})

	t.Run("GIVEN memory broker with a breaker delivery and a subscriber that works once every 5 calls", func(t *testing.T) {
		calls := 0
		worksEvery := 5
		handler := func(_ context.Context, t pubsub.Topic, m string) error {
			calls++

			if calls%worksEvery == 0 {
				return nil
			}

			return fmt.Errorf("fails")
		}

		strategy := retry.NewBreakerStrategy(4, time.Second)
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = retry.NewRetryMiddleware(broker, strategy, strategy)
		topic := pubsub.Topic("test")
		sub := pubsub.NewHandler(handler)

		require.NoError(t, broker.Subscribe(ctx, topic, sub))

		t.Run("WHEN publishing a message to such subscriber", func(t *testing.T) {
			err := broker.Publish(ctx, topic, "message")

			t.Run("THEN subscriber eventually receives the message after 5 attempts and a pause of 1s", func(t *testing.T) {
				require.NoError(t, err)
				require.GreaterOrEqual(t, calls, 5)
			})
		})
	})
}

type intermittentFailBroker struct {
	pubsub.Broker
	calls      int
	worksEvery int
	mu         sync.RWMutex
}

func (p *intermittentFailBroker) Publish(_ context.Context, _ pubsub.Topic, _ interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls++

	if modulo := p.calls % p.worksEvery; modulo == 0 {
		return nil
	}

	return errors.New("publish failed")
}
