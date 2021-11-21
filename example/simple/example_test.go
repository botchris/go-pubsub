package main_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/printer"
	"github.com/botchris/go-pubsub/middleware/recovery"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_EndToEnd(t *testing.T) {
	t.Run("GIVEN a memory broker with a recovery and printer middlewares and two subscribers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		t1 := pubsub.Topic("topic-1")
		t2 := pubsub.Topic("topic-2")

		rx := &lockedCounter{}
		panics := &lockedCounter{}

		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		writer := bytes.NewBuffer([]byte{})
		broker = printer.NewPrinterMiddleware(broker, writer)
		broker = recovery.NewRecoveryMiddleware(broker, func(ctx context.Context, p interface{}) error {
			panics.inc()

			return errors.New("panic recovery")
		})

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			rx.inc()

			return nil
		})

		h2 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m proto.Message) error {
			panic("boom")
		})

		require.NotNil(t, broker)
		require.NotNil(t, h1)
		require.NotNil(t, h2)

		_, err := broker.Subscribe(ctx, t1, h1)
		require.NoError(t, err)

		_, err = broker.Subscribe(ctx, t2, h2)
		require.NoError(t, err)

		t.Run("WHEN publishing a message to s1 THEN printer logs messages", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, t1, &emptypb.Empty{}))
			require.Equal(t, 1, rx.read())

			logs := writer.String()
			require.NotEmpty(t, logs)
			require.Contains(t, logs, "publishing")
			require.Contains(t, logs, "received")
		})

		t.Run("WHEN publishing a message to s2 THEN panic is recovered", func(t *testing.T) {
			require.NotPanics(t, func() {
				require.NoError(t, broker.Publish(ctx, t2, &emptypb.Empty{}))
			})

			require.EqualValues(t, panics.read(), 1)
		})
	})

	t.Run("without middlewares", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		topicID := pubsub.Topic("yolo-2")
		rx := &lockedCounter{}
		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			rx.inc()

			return nil
		})

		require.NotNil(t, broker)
		require.NotNil(t, h1)

		_, err := broker.Subscribe(ctx, topicID, h1)
		require.NoError(t, err)

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
