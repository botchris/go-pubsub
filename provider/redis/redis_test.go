package redis_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/codec"
	redisb "github.com/botchris/go-pubsub/provider/redis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestNewBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a redis broker with two subscriptions in the same group", func(t *testing.T) {
		groupID := "test"
		broker := prepareBroker(ctx, t)
		messages := []string{
			"test1",
			"test2",
			"test3",
		}

		q1 := &queue{}
		q2 := &queue{}

		h1 := pubsub.NewHandler(func(ctx context.Context, _ pubsub.Topic, m interface{}) error {
			q1.add(m.(string))

			return nil
		})

		h2 := pubsub.NewHandler(func(ctx context.Context, _ pubsub.Topic, m interface{}) error {
			q2.add(m.(string))

			return nil
		})

		_, err := broker.Subscribe(ctx, "test", h1, pubsub.WithGroup(groupID))
		require.NoError(t, err)

		_, err = broker.Subscribe(ctx, "test", h2, pubsub.WithGroup(groupID))
		require.NoError(t, err)

		// wait for redis server to be ready
		time.Sleep(time.Second)

		t.Run("WHEN publishing three messages", func(t *testing.T) {
			for _, message := range messages {
				require.NoError(t, broker.Publish(ctx, "test", message))
			}

			t.Run("THEN three messages are eventually received across subscriptions", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return q1.merge(q2).hasExactlyOnce(messages...)
				}, 5*time.Second, time.Millisecond*100)
			})
		})
	})

}

func TestPublishTrimsStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a stream holding entries older than the trim duration", func(t *testing.T) {
		s, err := miniredis.Run()
		require.NoError(t, err)

		client := redis.NewClient(&redis.Options{Addr: s.Addr()})
		old := time.Now().Add(-2 * time.Hour).UnixMilli()

		for i := 0; i < 10; i++ {
			require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{
				Stream: "stream-test",
				ID:     fmt.Sprintf("%d-%d", old, i),
				Values: map[string]interface{}{"event": "old", "attempt": 1},
			}).Err())
		}

		broker, err := redisb.NewBroker(ctx, redisb.WithAddress(s.Addr()), redisb.WithTrimDuration(time.Hour))
		require.NoError(t, err)

		t.Run("WHEN publishing a message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", []byte("new")))

			t.Run("THEN only the new message remains in the stream", func(t *testing.T) {
				require.EqualValues(t, 1, client.XLen(ctx, "stream-test").Val())
			})
		})
	})
}

func prepareBroker(ctx context.Context, t *testing.T) pubsub.Broker {
	t.Helper()

	s, err := miniredis.Run()
	require.NoError(t, err)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				s.FastForward(500 * time.Millisecond)
			}
		}
	}()

	broker, err := redisb.NewBroker(ctx, redisb.WithAddress(s.Addr()))
	require.NoError(t, err)

	broker = codec.NewCodecMiddleware(broker, codec.JSON)

	return broker
}

type queue struct {
	mu    sync.RWMutex
	items []string
}

func (q *queue) add(s string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, s)
}

func (q *queue) merge(q2 *queue) *queue {
	q.mu.RLock()
	q2.mu.RLock()

	defer q.mu.RUnlock()
	defer q2.mu.RUnlock()

	x := append(q.items, q2.items...)

	return &queue{
		items: x,
	}
}

func (q *queue) hasExactlyOnce(expected ...string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	seen := make(map[string]int)

	for _, m1 := range expected {
		for _, m2 := range q.items {
			if m1 == m2 {
				seen[m1]++

				if seen[m1] > 1 {
					return false
				}
			}
		}
	}

	return len(seen) == len(expected)
}
