package printer_test

import (
	"bytes"
	"context"
	"strings"
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
	t.Run("GIVEN a memory broker with a recovery middleware and two subscriptions", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		t1 := pubsub.Topic("topic-1")
		rx := &lockedCounter{}

		broker := memory.NewBroker()
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

			require.Eventually(t, func() bool {
				logs := writer.String()

				return rx.read() == 1 && strings.Contains(logs, "publishing") && strings.Contains(logs, "received")
			}, 5*time.Second, 100*time.Millisecond)
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
