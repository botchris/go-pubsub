package kmq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/codec"
	"github.com/botchris/go-pubsub/provider/kmq"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BenchmarkPublishJSONTenSubs(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	clientID := "test-client"
	topic := pubsub.Topic("topic")
	message := time.Now().String()

	broker, err := prepareJSONBroker(ctx, clientID, "")
	require.NoError(b, err)

	defer func() {
		require.NoError(b, broker.Shutdown(ctx))
	}()

	for i := 0; i < 10; i++ {
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m string) error {
			return nil
		})

		require.NoError(b, broker.Subscribe(ctx, topic, s))
	}

	time.Sleep(time.Second)

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		if pErr := broker.Publish(ctx, topic, message); pErr != nil {
			b.Fatal(pErr)
		}
	}
	b.StopTimer()
}

func BenchmarkPublishProtoTenSubs(b *testing.B) {
	ctx := context.Background()

	clientID := "test-client"
	topic := pubsub.Topic("topic")
	message := timestamppb.Now()

	broker, err := prepareProtoBroker(ctx, clientID, "")
	require.NoError(b, err)

	defer func() {
		require.NoError(b, broker.Shutdown(ctx))
	}()

	for i := 0; i < 10; i++ {
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m string) error {
			return nil
		})

		require.NoError(b, broker.Subscribe(ctx, topic, s))
	}

	time.Sleep(time.Second)

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		if pErr := broker.Publish(ctx, topic, message); pErr != nil {
			b.Fatal(pErr)
		}
	}
	b.StopTimer()
}

func TestSingleBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a broker with two subscribers to a topic", func(t *testing.T) {
		clientID := "test-client"
		broker, err := prepareJSONBroker(ctx, clientID, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, broker)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub1))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub2))

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

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a broker with two subscriber sharing the same queue", func(t *testing.T) {
		clientID := "test-client"
		broker, err := prepareJSONBroker(ctx, clientID, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, broker)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub1, pubsub.WithQueue("yolo")))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker.Subscribe(ctx, topic, sub2, pubsub.WithQueue("yolo")))

		// wait for server to ack async subscription
		time.Sleep(time.Second)

		t.Run("WHEN publishing 5 messages to such topic", func(t *testing.T) {
			send := []string{
				"test-message-1",
				"test-message-2",
				"test-message-3",
				"test-message-4",
				"test-message-5",
			}

			for _, msg := range send {
				require.NoError(t, broker.Publish(ctx, topic, msg))
			}

			t.Run("THEN all messages are eventually received sharded across subscribers", func(t *testing.T) {
				require.Eventually(t, func() bool {
					r1 := consumer1.received()
					r2 := consumer2.received()

					return r1.merge(r2).hasExactlyOnce(send...)
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
		broker1, err := prepareJSONBroker(ctx, clientID, "g1")
		require.NoError(t, err)
		require.NotNil(t, broker1)

		broker2, err := prepareJSONBroker(ctx, clientID, "g1")
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
		broker1, err := prepareJSONBroker(ctx, clientID, "g1")
		require.NoError(t, err)
		require.NotNil(t, broker1)

		broker2, err := prepareJSONBroker(ctx, clientID, "g2")
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

func prepareJSONBroker(ctx context.Context, clientID string, groupID string) (pubsub.Broker, error) {
	broker, err := kmq.NewBroker(ctx,
		kmq.WithClientID(clientID),
		kmq.WithGroupID(groupID),
		kmq.WithServerHost("localhost"),
		kmq.WithServerPort(50000),
	)

	if err != nil {
		return nil, err
	}

	return codec.NewCodecMiddleware(broker, codec.JSON), nil
}

func prepareProtoBroker(ctx context.Context, clientID string, groupID string) (pubsub.Broker, error) {
	broker, err := kmq.NewBroker(ctx,
		kmq.WithClientID(clientID),
		kmq.WithGroupID(groupID),
		kmq.WithServerHost("localhost"),
		kmq.WithServerPort(50000),
	)

	if err != nil {
		return nil, err
	}

	return codec.NewCodecMiddleware(broker, codec.Protobuf), nil
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

func (c *consumer) handle(_ context.Context, _ pubsub.Topic, msg string) error {
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
