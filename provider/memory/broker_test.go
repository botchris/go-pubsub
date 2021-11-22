package memory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type myMessage struct {
	body string
}

func BenchmarkPublish(b *testing.B) {
	ctx := context.Background()
	topic := pubsub.Topic("topic")
	message := myMessage{body: "hello"}

	broker := memory.NewBroker()

	h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
		return nil
	})

	_, err := broker.Subscribe(ctx, topic, h1)
	require.NoError(b, err)

	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		_ = broker.Publish(ctx, topic, message)
	}
	b.StopTimer()
}

func Test_Broker_Publish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("GIVEN a broker holding one subscriber WHEN publishing to topic THEN subscriber receives the message", func(t *testing.T) {
		broker := memory.NewBroker()
		rx := &lockedCounter{}
		topicID := pubsub.Topic("yolo")

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.Inc()

			return nil
		})

		_, err := broker.Subscribe(ctx, topicID, h1)
		require.NoError(t, err)

		require.NoError(t, broker.Publish(ctx, topicID, &CustomMessage{}))
		require.Eventually(t, func() bool {
			return rx.Read() == 1
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to one topic THEN subscriber receives only one message", func(t *testing.T) {
		broker := memory.NewBroker()
		rx := &lockedCounter{}
		topicA := pubsub.Topic("yolo-1")
		topicB := pubsub.Topic("yolo-2")

		s := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.Inc()

			return nil
		})

		_, err := broker.Subscribe(ctx, topicA, s)
		require.NoError(t, err)

		_, err = broker.Subscribe(ctx, topicB, s)
		require.NoError(t, err)

		require.NoError(t, broker.Publish(ctx, topicA, &DummyMessage{}))
		require.Eventually(t, func() bool {
			return rx.Read() == 1
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to all topics THEN subscriber receives multiple messages", func(t *testing.T) {
		broker := memory.NewBroker()
		rx := &lockedCounter{}
		topics := []pubsub.Topic{"yolo-1", "yolo-2"}

		h := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.Inc()

			return nil
		})

		_, err := broker.Subscribe(ctx, topics[0], h)
		require.NoError(t, err)

		_, err = broker.Subscribe(ctx, topics[1], h)
		require.NoError(t, err)

		require.NoError(t, broker.Publish(ctx, topics[0], &DummyMessage{}))
		require.NoError(t, broker.Publish(ctx, topics[1], &DummyMessage{}))

		require.Eventually(t, func() bool {
			return rx.Read() == 2
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("GIVEN a subscriber for a typed message WHEN publishing a message matching such type THEN subscriber receives the message", func(t *testing.T) {
		broker := memory.NewBroker()
		rx := &lockedCounter{}
		topic := pubsub.Topic("yolo-1")

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m *CustomMessage) error {
			rx.Inc()

			return nil
		})

		_, err := broker.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))

		require.Eventually(t, func() bool {
			return rx.Read() == 2
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("GIVEN a subscriber for a map-type message WHEN publishing a message matching such type THEN subscriber receives the message", func(t *testing.T) {
		broker := memory.NewBroker()
		rx := &lockedCounter{}
		topic := pubsub.Topic("yolo-1")

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m map[string]string) error {
			rx.Inc()

			return nil
		})

		_, err := broker.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		toSend := map[string]string{
			"key": "value",
		}

		require.NoError(t, broker.Publish(ctx, topic, toSend))
		require.NoError(t, broker.Publish(ctx, topic, toSend))

		require.Eventually(t, func() bool {
			return rx.Read() == 2
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("GIVEN a subscriber to a concrete pointer type WHEN publishing a messages not of that type THEN subscriber receives only desired type of the message", func(t *testing.T) {
		broker := memory.NewBroker()
		rx := &lockedCounter{}
		topic := pubsub.Topic("yolo-1")

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m *CustomMessage) error {
			rx.Inc()

			return nil
		})

		_, err := broker.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.NoError(t, broker.Publish(ctx, topic, &DummyMessage{}))

		require.Eventually(t, func() bool {
			return rx.Read() == 1
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("GIVEN a subscriber to an interface WHEN publishing a message implementing such interface THEN subscriber receives the message", func(t *testing.T) {
		broker := memory.NewBroker()
		rx := &lockedCounter{}
		topic := pubsub.Topic("yolo-1")

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m DummyInterface) error {
			rx.Inc()

			return nil
		})

		_, err := broker.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.NoError(t, broker.Publish(ctx, topic, &DummyMessage{}))

		require.Eventually(t, func() bool {
			return rx.Read() == 1
		}, 5*time.Second, 100*time.Millisecond)
	})
}

type CustomMessage struct {
	emptypb.Empty
}

type DummyInterface interface {
	proto.Message
	Dummy()
}

type DummyMessage struct {
	emptypb.Empty
}

func (d *DummyMessage) Dummy() {}

type lockedCounter struct {
	sync.RWMutex
	counter int
}

func (c *lockedCounter) Inc() {
	c.Lock()
	defer c.Unlock()

	c.counter++
}

func (c *lockedCounter) Read() int {
	c.Lock()
	defer c.Unlock()

	return c.counter
}
