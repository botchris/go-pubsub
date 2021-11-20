package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

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

	broker := memory.NewBroker(memory.NopSubscriberErrorHandler)

	s1 := pubsub.NewSubscriber(func(ctx context.Context, m interface{}) error {
		return nil
	})

	require.NoError(b, broker.Subscribe(ctx, topic, s1))

	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		_ = broker.Publish(ctx, topic, message)
	}
	b.StopTimer()
}

func Test_Broker_Subscriptions(t *testing.T) {
	t.Run("GIVEN an empty broker WHEN subscribing to topic THEN broker register such subscriber", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 0)

		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m interface{}) error { return nil })
		require.NoError(t, broker.Subscribe(ctx, "yolo", s))

		subscriptions, err = broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)
	})

	t.Run("GIVEN an empty broker WHEN subscribing to multiple topics THEN broker registers such subscriber", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		sub := func(ctx context.Context, t pubsub.Topic, m proto.Message) error { return nil }

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 0)

		s1 := pubsub.NewSubscriber(sub)
		s2 := pubsub.NewSubscriber(sub)

		require.NoError(t, broker.Subscribe(ctx, "yolo-1", s1))
		require.NoError(t, broker.Subscribe(ctx, "yolo-2", s2))

		subscriptions, err = broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 2)
	})

	t.Run("GIVEN an empty broker WHEN subscribing a typed message THEN broker registers such subscriber", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 0)

		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m *CustomMessage) error { return nil })
		require.NoError(t, broker.Subscribe(ctx, "yolo-1", s))

		subscriptions, err = broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)
	})
}

func Test_Broker_Unsubscribe(t *testing.T) {
	t.Run("GIVEN an broker with one topic and one subscriber WHEN unsubscribing THEN broker removes such subscriber", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		sub := func(ctx context.Context, t pubsub.Topic, m proto.Message) error { return nil }

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 0)

		s1 := pubsub.NewSubscriber(sub)
		require.NoError(t, broker.Subscribe(ctx, "yolo-1", s1))

		subscriptions, err = broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)
		require.Contains(t, subscriptions, pubsub.Topic("yolo-1"))

		require.NoError(t, broker.Unsubscribe(ctx, "yolo-1", s1))

		subscriptions, err = broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 0)
	})
}

func Test_Broker_Publish(t *testing.T) {
	t.Run("GIVEN a broker holding one subscriber WHEN publishing to topic THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		topicID := pubsub.Topic("yolo")
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.Inc()

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, topicID, s))

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)

		require.NoError(t, broker.Publish(ctx, topicID, &CustomMessage{}))
		require.EqualValues(t, 1, rx.Read())
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to one topic THEN subscriber receives only one message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}

		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.Inc()

			return nil
		})
		topicA := pubsub.Topic("yolo-1")
		topicB := pubsub.Topic("yolo-2")

		require.NoError(t, broker.Subscribe(ctx, topicA, s))
		require.NoError(t, broker.Subscribe(ctx, topicB, s))

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 2)
		require.Contains(t, subscriptions, topicA)
		require.Contains(t, subscriptions, topicB)

		require.NoError(t, broker.Publish(ctx, topicA, &DummyMessage{}))
		require.EqualValues(t, 1, rx.Read())
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to all topics THEN subscriber receives multiple messages", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.Inc()

			return nil
		})

		topics := []pubsub.Topic{"yolo-1", "yolo-2"}
		require.NoError(t, broker.Subscribe(ctx, topics[0], s))
		require.NoError(t, broker.Subscribe(ctx, topics[1], s))

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 2)
		require.Contains(t, subscriptions, topics[0])
		require.Contains(t, subscriptions, topics[1])

		require.NoError(t, broker.Publish(ctx, topics[0], &DummyMessage{}))
		require.NoError(t, broker.Publish(ctx, topics[1], &DummyMessage{}))
		require.EqualValues(t, 2, rx.Read())
	})

	t.Run("GIVEN a subscriber for a typed message WHEN publishing a message matching such type THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m *CustomMessage) error {
			rx.Inc()

			return nil
		})

		topic := pubsub.Topic("yolo-1")
		require.NoError(t, broker.Subscribe(ctx, topic, s))

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.EqualValues(t, 1, rx.Read())

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.EqualValues(t, 2, rx.Read())
	})

	t.Run("GIVEN a subscriber for a map-type message WHEN publishing a message matching such type THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m map[string]string) error {
			rx.Inc()

			return nil
		})

		topic := pubsub.Topic("yolo-1")
		require.NoError(t, broker.Subscribe(ctx, topic, s))

		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)

		toSend := map[string]string{
			"key": "value",
		}

		require.NoError(t, broker.Publish(ctx, topic, toSend))
		require.EqualValues(t, 1, rx.Read())

		require.NoError(t, broker.Publish(ctx, topic, toSend))
		require.EqualValues(t, 2, rx.Read())
	})

	t.Run("GIVEN a subscriber to a concrete pointer type WHEN publishing a messages not of that type THEN subscriber receives only desired type of the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m *CustomMessage) error {
			rx.Inc()

			return nil
		})
		topic := pubsub.Topic("yolo-1")

		require.NoError(t, broker.Subscribe(ctx, topic, s))
		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.EqualValues(t, 1, rx.Read())

		require.NoError(t, broker.Publish(ctx, topic, &DummyMessage{}))
		require.EqualValues(t, 1, rx.Read())
	})

	t.Run("GIVEN a subscriber to an interface WHEN publishing a message implementing such interface THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m DummyInterface) error {
			rx.Inc()

			return nil
		})
		topic := pubsub.Topic("yolo-1")

		require.NoError(t, broker.Subscribe(ctx, topic, s))
		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.EqualValues(t, 0, rx.Read())
		require.NoError(t, broker.Publish(ctx, topic, &DummyMessage{}))
		require.EqualValues(t, 1, rx.Read())
	})

	t.Run("GIVEN an ill subscriber WHEN publishing a message THEN error handling logic is triggered", func(t *testing.T) {
		ctx := context.Background()
		subError := fmt.Errorf("dummy error")
		errors := &lockedCounter{}
		errHandler := func(ctx context.Context, topic pubsub.Topic, s *pubsub.Subscriber, m interface{}, err error) {
			errors.Inc()
		}

		broker := memory.NewBroker(errHandler)
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m *CustomMessage) error { return subError })
		topic := pubsub.Topic("yolo-1")

		require.NoError(t, broker.Subscribe(ctx, topic, s))
		subscriptions, err := broker.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)

		require.NoError(t, broker.Publish(ctx, topic, &CustomMessage{}))
		require.EqualValues(t, 1, errors.Read())
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
