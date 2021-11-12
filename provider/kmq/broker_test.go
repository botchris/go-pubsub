package kmq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/kmq"
	"github.com/stretchr/testify/require"
)

func TestSingleBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a broker with two subscribers to a topic", func(t *testing.T) {
		broker, err := prepareBroker(ctx)
		require.NoError(t, err)
		require.NotNil(t, broker)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub1))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub2))

		// wait for server to ack subscription async
		time.Sleep(time.Second)

		t.Run("WHEN publishing a message to topic X", func(t *testing.T) {
			msg := "test-message"
			require.NoError(t, broker.Publish(ctx, topic, msg))

			t.Run("THEN subscribers eventually receives the message the same message", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return consumer1.hasExactlyOnce(msg) && consumer2.hasExactlyOnce(msg)
				}, 3*time.Second, time.Millisecond*100)
			})
		})
	})
}

func prepareBroker(ctx context.Context) (pubsub.Broker, error) {
	encoder := func(msg interface{}) ([]byte, error) { return []byte(msg.(string)), nil }
	decoder := func(data []byte) (interface{}, error) { return string(data), nil }

	return kmq.NewBroker(ctx,
		kmq.WithClientID("test-client"),
		kmq.WithEncoder(encoder),
		kmq.WithDecoder(decoder),
		kmq.WithServerHost("localhost"),
		kmq.WithServerPort(50000),
	)
}

type consumer struct {
	rcv []string
	mu  sync.RWMutex
}

func (c *consumer) handle(_ context.Context, msg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.rcv = append(c.rcv, msg)

	return nil
}

func (c *consumer) hasExactlyOnce(expected ...string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	seen := make(map[string]int)

	for _, m1 := range expected {
		for _, m2 := range c.rcv {
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
