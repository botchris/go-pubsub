package printer_test

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
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestNewPrinterMiddleware(t *testing.T) {
	t.Run("GIVEN a memory broker with a recovery middleware and two subscribers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		t1 := pubsub.Topic("topic-1")
		rx := &lockedCounter{}

		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		writer := bytes.NewBuffer([]byte{})
		broker = printer.NewPrinterMiddleware(broker, writer)

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m interface{}) error {
			rx.inc()

			return nil
		})

		require.NotNil(t, broker)
		require.NotNil(t, h1)

		_, err := broker.Subscribe(ctx, t1, h1)
		require.NoError(t, err)

		t.Run("WHEN publishing a message to s1 THEN printer logs messages", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, t1, &emptypb.Empty{}))
			require.Equal(t, 1, rx.read())

			logs := writer.String()
			require.NotEmpty(t, logs)
			require.Contains(t, logs, "publishing")
			require.Contains(t, logs, "received")
		})
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
