package main_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/printer"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_EndToEnd(t *testing.T) {
	t.Run("with interceptors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		topicID := pubsub.Topic("yolo")
		writer := bytes.NewBuffer([]byte{})
		broker = printer.NewPrinterMiddleware(broker, writer)
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.inc()

			return nil
		})

		require.NotNil(t, broker)
		require.NotNil(t, s)

		require.NoError(t, broker.Subscribe(ctx, topicID, s))
		require.NoError(t, broker.Publish(ctx, topicID, &emptypb.Empty{}))
		require.Equal(t, 1, rx.read())

		logs := writer.String()
		require.NotEmpty(t, logs)
		require.Contains(t, logs, "publishing")
		require.Contains(t, logs, "received")
	})

	t.Run("without interceptors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		topicID := pubsub.Topic("yolo-2")
		rx := &lockedCounter{}
		s := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			rx.inc()

			return nil
		})

		require.NotNil(t, broker)
		require.NotNil(t, s)

		require.NoError(t, broker.Subscribe(ctx, topicID, s))
		require.NoError(t, broker.Publish(ctx, topicID, &emptypb.Empty{}))
		require.Equal(t, 1, rx.read())
	})
}

type lockedCounter struct {
	sync.RWMutex
	counter int
}

func (c *lockedCounter) inc() {
	c.Lock()
	defer c.Unlock()

	c.counter++
}

func (c *lockedCounter) read() int {
	c.Lock()
	defer c.Unlock()

	return c.counter
}
