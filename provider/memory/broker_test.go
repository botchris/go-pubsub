package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ChristopherCastro/go-pubsub"
	"github.com/ChristopherCastro/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_Broker_Subscribe(t *testing.T) {
	t.Run("GIVEN an empty broker WHEN subscribing to topic THEN broker register such subscriber", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)

		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 0)

		s := pubsub.NewSubscriber(func(ctx context.Context, m proto.Message) error { return nil })
		require.NoError(t, broker.Subscribe(ctx, s, "yolo"))

		topics, err = broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 1)
	})

	t.Run("GIVEN an empty broker WHEN subscribing to multiple topics THEN broker registers such subscriber AND topics are created", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		sub := func(ctx context.Context, m proto.Message) error { return nil }

		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 0)

		s1 := pubsub.NewSubscriber(sub)
		s2 := pubsub.NewSubscriber(sub)

		require.NoError(t, broker.Subscribe(ctx, s1, "yolo-1"))
		require.NoError(t, broker.Subscribe(ctx, s2, "yolo-2"))

		topics, err = broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 2)
	})

	t.Run("GIVEN an empty broker WHEN subscribing a typed message THEN broker registers such subscriber", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)

		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 0)

		s := pubsub.NewSubscriber(func(ctx context.Context, m *CustomMessage) error { return nil })
		require.NoError(t, broker.Subscribe(ctx, s, "yolo-1"))

		topics, err = broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 1)
	})
}

func Test_Broker_Publish(t *testing.T) {
	t.Run("GIVEN a broker holding one subscriber WHEN publishing to topic THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		topicID := pubsub.Topic("yolo")
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, m proto.Message) error {
			rx.Inc()

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, s, topicID))

		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 1)

		require.NoError(t, broker.Publish(ctx, &CustomMessage{}, topicID))
		require.EqualValues(t, 1, rx.Read())
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to one topic THEN subscriber receives only one message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}

		s := pubsub.NewSubscriber(func(ctx context.Context, m proto.Message) error {
			rx.Inc()

			return nil
		})
		topicA := pubsub.Topic("yolo-1")
		topicB := pubsub.Topic("yolo-2")

		require.NoError(t, broker.Subscribe(ctx, s, topicA))
		require.NoError(t, broker.Subscribe(ctx, s, topicB))

		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 2)

		require.NoError(t, broker.Publish(ctx, &DummyMessage{}, topicA))
		require.EqualValues(t, 1, rx.Read())
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to all topics THEN subscriber receives multiple messages", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, m proto.Message) error {
			rx.Inc()

			return nil
		})

		topics := []pubsub.Topic{"yolo-1", "yolo-2"}
		require.NoError(t, broker.Subscribe(ctx, s, topics[0]))
		require.NoError(t, broker.Subscribe(ctx, s, topics[1]))

		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 2)

		require.NoError(t, broker.Publish(ctx, &DummyMessage{}, topics[0]))
		require.NoError(t, broker.Publish(ctx, &DummyMessage{}, topics[1]))
		require.EqualValues(t, 2, rx.Read())
	})

	t.Run("GIVEN a subscriber for a typed message WHEN publishing a message matching such type THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, m *CustomMessage) error {
			rx.Inc()

			return nil
		})
		topic := pubsub.Topic("yolo-1")

		require.NoError(t, broker.Subscribe(ctx, s, topic))

		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 1)

		require.NoError(t, broker.Publish(ctx, &CustomMessage{}, topic))
		require.EqualValues(t, 1, rx.Read())

		require.NoError(t, broker.Publish(ctx, &CustomMessage{}, topic))
		require.EqualValues(t, 2, rx.Read())
	})

	t.Run("GIVEN a subscriber to a concrete pointer type WHEN publishing a messages not of that type THEN subscriber receives only desired type of the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, m *CustomMessage) error {
			rx.Inc()

			return nil
		})
		topic := pubsub.Topic("yolo-1")

		require.NoError(t, broker.Subscribe(ctx, s, topic))
		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 1)

		require.NoError(t, broker.Publish(ctx, &CustomMessage{}, topic))
		require.EqualValues(t, 1, rx.Read())

		require.NoError(t, broker.Publish(ctx, &DummyMessage{}, topic))
		require.EqualValues(t, 1, rx.Read())
	})

	t.Run("GIVEN a subscriber to an interface WHEN publishing a message implementing such interface THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, m DummyInterface) error {
			rx.Inc()

			return nil
		})
		topic := pubsub.Topic("yolo-1")

		require.NoError(t, broker.Subscribe(ctx, s, topic))
		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 1)

		require.NoError(t, broker.Publish(ctx, &CustomMessage{}, topic))
		require.EqualValues(t, 0, rx.Read())
		require.NoError(t, broker.Publish(ctx, &DummyMessage{}, topic))
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
		s := pubsub.NewSubscriber(func(ctx context.Context, m *CustomMessage) error { return subError })
		topic := pubsub.Topic("yolo-1")

		require.NoError(t, broker.Subscribe(ctx, s, topic))
		topics, err := broker.Topics(ctx)
		require.NoError(t, err)
		require.Len(t, topics, 1)

		require.NoError(t, broker.Publish(ctx, &CustomMessage{}, topic))
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
