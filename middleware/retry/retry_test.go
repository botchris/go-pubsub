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
		broker = retry.NewRetryMiddleware(broker, retry.WithPublishStrategy(strategy), retry.WithDeliverStrategy(strategy))

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
		broker = retry.NewRetryMiddleware(broker, retry.WithPublishStrategy(strategy), retry.WithDeliverStrategy(strategy))

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

func TestDeliveryInterception(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("GIVEN memory broker with an exponential backoff delivery and a handler that works once every 5 calls", func(t *testing.T) {
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

		broker := memory.NewBroker()
		broker = retry.NewRetryMiddleware(broker, retry.WithPublishStrategy(strategy), retry.WithDeliverStrategy(strategy))
		topic := pubsub.Topic("test")

		h1 := pubsub.NewHandler(handler)
		_, err := broker.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		t.Run("WHEN publishing a message to such handler", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, topic, "message"))

			t.Run("THEN handler eventually receives the message after 5 attempts", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return calls >= worksEvery
				}, 5*time.Second, 100*time.Millisecond)
			})
		})
	})

	t.Run("GIVEN memory broker with a breaker delivery and a handler that works once every 5 calls", func(t *testing.T) {
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
		broker := memory.NewBroker()
		broker = retry.NewRetryMiddleware(broker, retry.WithPublishStrategy(strategy), retry.WithDeliverStrategy(strategy))
		topic := pubsub.Topic("test")

		h1 := pubsub.NewHandler(handler)
		_, err := broker.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		t.Run("WHEN publishing a message to such handler", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, topic, "message"))

			t.Run("THEN handler eventually receives the message after 5 attempts and a pause of 1s", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return calls >= worksEvery
				}, 5*time.Second, 100*time.Millisecond)
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
