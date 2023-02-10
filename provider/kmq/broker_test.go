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

	broker, err := prepareJSONBroker(ctx, clientID)
	require.NoError(b, err)

	defer func() {
		require.NoError(b, broker.Shutdown(ctx))
	}()

	for i := 0; i < 10; i++ {
		h := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m string) error {
			return nil
		})

		_, sErr := broker.Subscribe(ctx, topic, h)
		require.NoError(b, sErr)
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

	broker, err := prepareProtoBroker(ctx, clientID)
	require.NoError(b, err)

	defer func() {
		require.NoError(b, broker.Shutdown(ctx))
	}()

	for i := 0; i < 10; i++ {
		h := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m string) error {
			return nil
		})

		_, sErr := broker.Subscribe(ctx, topic, h)
		require.NoError(b, sErr)
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
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	t.Run("GIVEN a broker with two subscriptions to the same topic but for different groups", func(t *testing.T) {
		clientID := "test-client"
		topic := pubsub.Topic("test-topic")
		broker, err := prepareJSONBroker(ctx, clientID)
		require.NoError(t, err)
		require.NotNil(t, broker)

		consumer1 := &consumer{}
		h1 := pubsub.NewHandler(consumer1.handle)
		s1, err := broker.Subscribe(ctx, topic, h1, pubsub.WithGroup("g1"))
		require.NoError(t, err)
		require.EqualValues(t, "g1", s1.Options().Group)

		consumer2 := &consumer{}
		h2 := pubsub.NewHandler(consumer2.handle)
		s2, err := broker.Subscribe(ctx, topic, h2, pubsub.WithGroup("g2"))
		require.NoError(t, err)
		require.EqualValues(t, "g2", s2.Options().Group)

		// wait for server to ack async subscription
		time.Sleep(time.Second)

		t.Run("WHEN publishing two messages to topic", func(t *testing.T) {
			msgs := []string{
				"message-1",
				"message-2",
			}

			for _, m := range msgs {
				require.NoError(t, broker.Publish(ctx, topic, m))
			}

			t.Run("THEN both subscriptions eventually receives the same messages", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return consumer1.received().hasExactlyOnce(msgs...) && consumer2.received().hasExactlyOnce(msgs...)
				}, 3*time.Second, time.Millisecond*100)
			})
		})
	})
}

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a broker with two subscriptions sharing the same queue", func(t *testing.T) {
		clientID := "test-client"
		gid := uuid.New()

		broker, err := prepareJSONBroker(ctx, clientID)
		require.NoError(t, err)
		require.NotNil(t, broker)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		h1 := pubsub.NewHandler(consumer1.handle)
		_, err = broker.Subscribe(ctx, topic, h1, pubsub.WithGroup(gid))
		require.NoError(t, err)

		consumer2 := &consumer{}
		h2 := pubsub.NewHandler(consumer2.handle)
		_, err = broker.Subscribe(ctx, topic, h2, pubsub.WithGroup(gid))
		require.NoError(t, err)

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

			t.Run("THEN all messages are eventually received sharded across subscriptions", func(t *testing.T) {
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

	t.Run("GIVEN two brokers with the same id AND with one subscription each to the same topic", func(t *testing.T) {
		clientID := "test-client"
		gid := uuid.New()

		broker1, err := prepareJSONBroker(ctx, clientID, gid)
		require.NoError(t, err)
		require.NotNil(t, broker1)

		broker2, err := prepareJSONBroker(ctx, clientID, gid)
		require.NoError(t, err)
		require.NotNil(t, broker2)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		h1 := pubsub.NewHandler(consumer1.handle)
		_, err = broker1.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		consumer2 := &consumer{}
		h2 := pubsub.NewHandler(consumer2.handle)
		_, err = broker2.Subscribe(ctx, topic, h2)
		require.NoError(t, err)

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

			t.Run("THEN eventually all messages are received across both subscriptions", func(t *testing.T) {
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

	t.Run("GIVEN two brokers with the distinct ids and with one subscription each to the same topic", func(t *testing.T) {
		clientID := "test-client"
		broker1, err := prepareJSONBroker(ctx, clientID)
		require.NoError(t, err)
		require.NotNil(t, broker1)

		broker2, err := prepareJSONBroker(ctx, clientID)
		require.NoError(t, err)
		require.NotNil(t, broker2)

		topic := pubsub.Topic("test-topic")

		consumer1 := &consumer{}
		h1 := pubsub.NewHandler(consumer1.handle)
		_, err = broker1.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		consumer2 := &consumer{}
		h2 := pubsub.NewHandler(consumer2.handle)
		_, err = broker2.Subscribe(ctx, topic, h2)
		require.NoError(t, err)

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

			t.Run("THEN eventually all messages are received across both subscriptions", func(t *testing.T) {
				require.Eventually(t, func() bool {
					r1 := consumer1.received()
					r2 := consumer2.received()

					return r1.hasExactlyOnce(send...) && r2.hasExactlyOnce(send...)
				}, 3*time.Second, time.Millisecond*100)
			})
		})
	})
}

func prepareJSONBroker(ctx context.Context, clientID string, gid ...string) (pubsub.Broker, error) {
	groupID := uuid.New()
	if len(gid) > 0 {
		groupID = gid[0]
	}

	broker, err := kmq.NewBroker(ctx,
		kmq.WithClientID(clientID),
		kmq.WithServerHost("localhost"),
		kmq.WithServerPort(50000),
		kmq.WithGroupID(groupID),
	)

	if err != nil {
		return nil, err
	}

	return codec.NewCodecMiddleware(broker, codec.JSON), nil
}

func prepareProtoBroker(ctx context.Context, clientID string, gid ...string) (pubsub.Broker, error) {
	groupID := uuid.New()
	if len(gid) > 0 {
		groupID = gid[0]
	}

	broker, err := kmq.NewBroker(ctx,
		kmq.WithClientID(clientID),
		kmq.WithServerHost("localhost"),
		kmq.WithServerPort(50000),
		kmq.WithGroupID(groupID),
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
