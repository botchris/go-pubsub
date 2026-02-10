package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/pool"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
)

func TestNewPoolMiddleware(t *testing.T) {
	t.Run("GIVEN a memory broker with a pool middleware and a subscriber", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		broker := pool.NewPoolMiddleware(memory.NewBroker(), 100)
		topic := pubsub.Topic("topic-1")
		rx := &stack{}

		handler := pubsub.NewHandler(func(ctx context.Context, topic pubsub.Topic, message interface{}) error {
			rx.append(message)

			return nil
		})

		_, err := broker.Subscribe(ctx, topic, handler)
		require.NoError(t, err)

		t.Run("WHEN publishing 1000 messages", func(t *testing.T) {
			maxMessages := 1000

			for i := 0; i < maxMessages; i++ {
				pErr := broker.Publish(ctx, topic, i)
				require.NoError(t, pErr)
			}

			t.Run("THEN all messages are eventually received by the subscriber", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return rx.size() == maxMessages
				}, 5*time.Second, 100*time.Millisecond)
			})
		})
	})

	t.Run("GIVEN a memory broker with a context pipe functions option AND a context with a value", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		ctx = context.WithValue(ctx, "foo", "bar")

		broker := pool.NewPoolMiddleware(
			memory.NewBroker(),
			100,
			pool.WithCtxMetadataFunc(func(pubCtx context.Context) map[string]any {
				return map[string]any{
					"foo": pubCtx.Value("foo"),
				}
			}),
		)
		topic := pubsub.Topic("topic-1")
		rx := &stack{}

		mux := &sync.Mutex{}
		poolCtxMDExists := false

		handler := pubsub.NewHandler(func(ctx context.Context, topic pubsub.Topic, message interface{}) error {
			rx.append(message)

			mux.Lock()
			defer mux.Unlock()

			md := pool.MetadataFromContext(ctx)
			if val, exists := md["foo"]; exists && val == "bar" {
				poolCtxMDExists = true
			}

			return nil
		})

		_, err := broker.Subscribe(ctx, topic, handler)
		require.NoError(t, err)

		t.Run("WHEN publishing a messages", func(t *testing.T) {
			pErr := broker.Publish(ctx, topic, "test message")
			require.NoError(t, pErr)

			t.Run("THEN pool metadata key and value exist in the subscribe ctx", func(t *testing.T) {
				require.Eventually(t, func() bool {
					mux.Lock()
					defer mux.Unlock()

					return rx.size() == 1 && poolCtxMDExists
				}, 5*time.Second, 100*time.Millisecond)
			})
		})
	})
}

type stack struct {
	sync.RWMutex
	messages []interface{}
}

func (c *stack) append(m interface{}) {
	c.Lock()
	defer c.Unlock()

	c.messages = append(c.messages, m)
}

func (c *stack) size() int {
	c.Lock()
	defer c.Unlock()

	return len(c.messages)
}
