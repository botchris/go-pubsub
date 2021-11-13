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
		clientID := "test-client"
		broker, err := prepareBroker(ctx, clientID, "")
		require.NoError(t, err)
		require.NotNil(t, broker)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub1))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub2))

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)
		require.Contains(t, subscriptions, topic)
		require.Len(t, subscriptions[topic], 2)

		// wait for server to ack async subscription
		time.Sleep(time.Second)

		t.Run("WHEN publishing a message to topic X", func(t *testing.T) {
			msg := "test-message"
			require.NoError(t, broker.Publish(ctx, topic, msg))

			t.Run("THEN subscribers eventually receives the same message", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return consumer1.received().hasExactlyOnce(msg) && consumer2.received().hasExactlyOnce(msg)
				}, 3*time.Second, time.Millisecond*100)
			})
		})
	})
}

func TestMultiInstanceBroker(t *testing.T) {
	// This test simulates multiple instances of the same application sharing the handling of available messages.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN two brokers with the same id and with one subscriber each to the same topic", func(t *testing.T) {
		clientID := "test-client"
		broker1, err := prepareBroker(ctx, clientID, "g1")
		require.NoError(t, err)
		require.NotNil(t, broker1)

		broker2, err := prepareBroker(ctx, clientID, "g1")
		require.NoError(t, err)
		require.NotNil(t, broker2)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker1.Subscribe(ctx, topic, sub1))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker2.Subscribe(ctx, topic, sub2))

		// wait for server to ack async subscription
		time.Sleep(time.Second)

		t.Run("WHEN publishing N message to the topic", func(t *testing.T) {
			send := []string{
				"test-message-1",
				"test-message-2",
				"test-message-3",
				"test-message-4",
				"test-message-5",
			}

			for _, msg := range send {
				require.NoError(t, broker1.Publish(ctx, topic, msg))
			}

			t.Run("THEN eventually all messages are received across both subscribers", func(t *testing.T) {
				require.Eventually(t, func() bool {
					r1 := consumer1.received()
					r2 := consumer2.received()

					return r1.merge(r2).hasExactlyOnce(send...)
				}, 3*time.Second, time.Millisecond*100)
			})
		})
	})
}

func TestMultiHostBroker(t *testing.T) {
	// This test simulates multiple applications reading from the same topics
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN two brokers with the distinct ids and with one subscriber each to the same topic", func(t *testing.T) {
		clientID := "test-client"
		broker1, err := prepareBroker(ctx, clientID, "g1")
		require.NoError(t, err)
		require.NotNil(t, broker1)

		broker2, err := prepareBroker(ctx, clientID, "g2")
		require.NoError(t, err)
		require.NotNil(t, broker2)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker1.Subscribe(ctx, topic, sub1))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker2.Subscribe(ctx, topic, sub2))

		// wait for server to ack async subscription
		time.Sleep(time.Second)

		t.Run("WHEN publishing N message to the topic", func(t *testing.T) {
			send := []string{
				"test-message-1",
				"test-message-2",
				"test-message-3",
				"test-message-4",
				"test-message-5",
			}

			for _, msg := range send {
				require.NoError(t, broker1.Publish(ctx, topic, msg))
			}

			t.Run("THEN eventually all messages are received across both subscribers", func(t *testing.T) {
				require.Eventually(t, func() bool {
					r1 := consumer1.received()
					r2 := consumer2.received()

					return r1.hasExactlyOnce(send...) && r2.hasExactlyOnce(send...)
				}, 3*time.Second, time.Millisecond*100)
			})
		})
	})
}

func prepareBroker(ctx context.Context, clientID string, groupID string) (pubsub.Broker, error) {
	encoder := func(msg interface{}) ([]byte, error) { return []byte(msg.(string)), nil }
	decoder := func(data []byte) (interface{}, error) { return string(data), nil }

	return kmq.NewBroker(ctx,
		kmq.WithClientID(clientID),
		kmq.WithGroupID(groupID),
		kmq.WithEncoder(encoder),
		kmq.WithDecoder(decoder),
		kmq.WithServerHost("localhost"),
		kmq.WithServerPort(50000),
	)
}

type consumer struct {
	rcv queue
	mu  sync.RWMutex
}

type queue []string

func (q queue) merge(q2 queue) queue {
	x := append(q, q2...)

	return x
}

func (q queue) hasExactlyOnce(expected ...string) bool {
	seen := make(map[string]int)

	for _, m1 := range expected {
		for _, m2 := range q {
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

func (c *consumer) handle(_ context.Context, msg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.rcv = append(c.rcv, msg)

	return nil
}

func (c *consumer) received() queue {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]string, len(c.rcv))
	copy(out, c.rcv)

	return out
}
