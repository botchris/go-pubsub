package pubsub_test

import (
	"context"
	"sync"
	"testing"

	"github.com/ChristopherCastro/go-pubsub"
	"github.com/stretchr/testify/require"
)

func Test_Broker_Subscribe(t *testing.T) {
	t.Run("GIVEN an empty broker WHEN subscribing to topic THEN broker register such subscriber", func(t *testing.T) {
		ctx := context.Background()
		broker := pubsub.NewBroker()

		require.Len(t, broker.Topics(), 0)
		broker.Subscribe(ctx, func(ctx context.Context, m interface{}) {}, "yolo")
		require.Len(t, broker.Topics(), 1)
	})

	t.Run("GIVEN an empty broker WHEN subscribing to multiple topics THEN broker register such subscriber AND topics are created", func(t *testing.T) {
		ctx := context.Background()
		broker := pubsub.NewBroker()
		sub := func(ctx context.Context, m interface{}) {}

		require.Len(t, broker.Topics(), 0)
		broker.Subscribe(ctx, sub, "yolo-1")
		broker.Subscribe(ctx, sub, "yolo-2")
		require.Len(t, broker.Topics(), 2)
	})
}

func Test_Broker_Publish(t *testing.T) {
	t.Run("GIVEN a broker holding one subscriber WHEN publishing to topic THEN subscriber receives the message", func(t *testing.T) {
		ctx := context.Background()
		broker := pubsub.NewBroker()
		topicID := pubsub.TopicID("yolo")
		rx := &counter{}
		sub := func(ctx context.Context, m interface{}) {
			rx.inc()
		}

		broker.Subscribe(ctx, sub, topicID)
		require.Len(t, broker.Topics(), 1)

		broker.Publish(ctx, "dummy message", topicID)
		require.EqualValues(t, 1, rx.read())
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to one topic THEN subscriber receives only one message", func(t *testing.T) {
		ctx := context.Background()
		broker := pubsub.NewBroker()
		rx := &counter{}
		sub := func(ctx context.Context, m interface{}) {
			rx.inc()
		}

		broker.Subscribe(ctx, sub, "yolo-1", "yolo-2")
		require.Len(t, broker.Topics(), 2)

		broker.Publish(ctx, "dummy message", "yolo-1")
		require.EqualValues(t, 1, rx.read())
	})

	t.Run("GIVEN a broker holding one subscriber on multiple topic WHEN publishing to all topics THEN subscriber receives multiple messages", func(t *testing.T) {
		ctx := context.Background()
		broker := pubsub.NewBroker()
		rx := &counter{}
		sub := func(ctx context.Context, m interface{}) {
			rx.inc()
		}

		broker.Subscribe(ctx, sub, "yolo-1", "yolo-2")
		require.Len(t, broker.Topics(), 2)

		broker.Publish(ctx, "dummy message", "yolo-1", "yolo-2")
		require.EqualValues(t, 2, rx.read())
	})
}

type counter struct {
	sync.RWMutex
	counter int
}

func (c *counter) inc() {
	c.Lock()
	defer c.Unlock()

	c.counter++
}

func (c *counter) read() int {
	c.Lock()
	defer c.Unlock()

	return c.counter
}
