package memory_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/assert"
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

	t.Run("GIVEN a broker holding one subscription WHEN publishing to topic THEN subscription receives the message", func(t *testing.T) {
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

	t.Run("GIVEN a broker holding one subscription on multiple topic WHEN publishing to one topic THEN subscription receives only one message", func(t *testing.T) {
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

	t.Run("GIVEN a broker holding one subscription on multiple topic WHEN publishing to all topics THEN subscription receives multiple messages", func(t *testing.T) {
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

	t.Run("GIVEN a subscription for a typed message WHEN publishing a message matching such type THEN subscription receives the message", func(t *testing.T) {
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

	t.Run("GIVEN a subscription for a map-type message WHEN publishing a message matching such type THEN subscription receives the message", func(t *testing.T) {
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

	t.Run("GIVEN a subscription to a concrete pointer type WHEN publishing a messages not of that type THEN subscription receives only desired type of the message", func(t *testing.T) {
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

	t.Run("GIVEN a subscription to an interface WHEN publishing a message implementing such interface THEN subscription receives the message", func(t *testing.T) {
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

	t.Run("GIVEN a message to publish", func(t *testing.T) {
		ctx = context.Background()
		broker := memory.NewBroker()

		topic := pubsub.Topic("non-existent-topic")
		message := "test message"

		t.Run("WHEN the topic does not exist", func(t *testing.T) {
			err := broker.Publish(ctx, topic, message)

			t.Run("THEN it creates the topic and publishes successfully", func(t *testing.T) {
				require.NoError(t, err)
			})
		})
	})

	t.Run("GIVEN a message to publish", func(t *testing.T) {
		ctx = context.Background()
		broker := memory.NewBroker()

		topic := pubsub.Topic("no-subscribers-topic")
		message := "test message"

		t.Run("WHEN there are no subscribers", func(t *testing.T) {
			err := broker.Publish(ctx, topic, message)

			t.Run("THEN it publishes successfully without errors", func(t *testing.T) {
				require.NoError(t, err)
			})
		})
	})

	t.Run("GIVEN a message to publish", func(t *testing.T) {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		broker := memory.NewBroker()

		topic := pubsub.Topic("with-subscriber-topic")
		receivedMessages := make(chan interface{}, 1)

		handler := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			receivedMessages <- m
			return nil
		})

		_, err := broker.Subscribe(ctx, topic, handler)
		require.NoError(t, err)

		testMessage := "hello"

		t.Run("WHEN there is a subscriber", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, topic, testMessage))

			t.Run("THEN the subscriber receives the message", func(t *testing.T) {
				select {
				case msg := <-receivedMessages:
					require.Equal(t, testMessage, msg)
				case <-ctx.Done():
					t.Fatal("Did not receive message in time")
				}
			})
		})
	})

	t.Run("GIVEN a message to publish", func(t *testing.T) {
		ctx = context.Background()
		broker := memory.NewBroker() // Broker without publish errors enabled
		topic := pubsub.Topic("subscriber-error-no-publish-error")
		message := "test message"

		handler := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			return errors.New("subscriber error")
		})

		_, err := broker.Subscribe(ctx, topic, handler)
		require.NoError(t, err)

		t.Run("WHEN a subscriber returns an error BUT publish errors are not enabled", func(t *testing.T) {
			publishErr := broker.Publish(ctx, topic, message)

			t.Run("THEN publish does not return an error", func(t *testing.T) {
				require.NoError(t, publishErr)
			})
		})
	})

	t.Run("GIVEN a message to publish", func(t *testing.T) {
		ctx = context.Background()

		brokerWithErrors := memory.NewBroker(memory.WithPublishErrors())
		topic := pubsub.Topic("subscriber-fail-topic")
		message := "test message"

		handler := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			return errors.New("failed to process message")
		})

		_, err := brokerWithErrors.Subscribe(ctx, topic, handler)
		require.NoError(t, err)

		t.Run("WHEN publish errors are enabled and a subscriber fails", func(t *testing.T) {
			publishErr := brokerWithErrors.Publish(ctx, topic, message)

			t.Run("THEN it returns an error", func(t *testing.T) {
				require.Error(t, publishErr)
			})
		})
	})

	t.Run("GIVEN a message to publish", func(t *testing.T) {
		ctx = context.Background()

		brokerWithErrors := memory.NewBroker(memory.WithPublishErrors())
		topic := pubsub.Topic("mixed-results-topic")
		message := "test message"

		successHandler := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			return nil
		})
		failHandler := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			return errors.New("failed to process message")
		})

		_, err := brokerWithErrors.Subscribe(ctx, topic, successHandler)
		require.NoError(t, err)
		_, err = brokerWithErrors.Subscribe(ctx, topic, failHandler)
		require.NoError(t, err)

		t.Run("WHEN publish errors are enabled and multiple subscribers with mixed results", func(t *testing.T) {
			publishErr := brokerWithErrors.Publish(ctx, topic, message)

			t.Run("THEN it returns an error", func(t *testing.T) {
				require.Error(t, publishErr)
			})
		})
	})

	t.Run("GIVEN a message to publish", func(t *testing.T) {
		ctx = context.Background()

		brokerWithErrors := memory.NewBroker(memory.WithPublishErrors())
		topic := pubsub.Topic("mixed-results-topic")
		message := "test message"

		failHandler := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			return assert.AnError
		})

		otherErr := errors.New("other error")

		otherFailHandler := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			return otherErr
		})

		_, err := brokerWithErrors.Subscribe(ctx, topic, failHandler)
		require.NoError(t, err)
		_, err = brokerWithErrors.Subscribe(ctx, topic, otherFailHandler)
		require.NoError(t, err)

		t.Run("WHEN publish errors are enabled and multiple subscribers returning error", func(t *testing.T) {
			publishErr := brokerWithErrors.Publish(ctx, topic, message)

			t.Run("THEN it returns both errors", func(t *testing.T) {
				require.Error(t, publishErr)

				assert.ErrorIs(t, publishErr, assert.AnError)
				assert.ErrorIs(t, publishErr, otherErr)

				fmt.Println(publishErr)
			})
		})
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
