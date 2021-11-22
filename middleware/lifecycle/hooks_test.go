package lifecycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/lifecycle"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
)

func TestHooksMiddleware(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a memory broker with OnPublish, OnDeliver and OnShutdown hooks defined and one subscription", func(t *testing.T) {
		publishCalls := 0
		deliverCalls := 0
		shutdownCalls := 0

		broker := memory.NewBroker()
		broker = lifecycle.NewLifecycleMiddleware(broker, lifecycle.Hooks{
			OnPublish: func(ctx context.Context, topic pubsub.Topic, m interface{}, err error) {
				publishCalls++
			},
			OnDeliver: func(ctx context.Context, topic pubsub.Topic, m interface{}, err error) {
				deliverCalls++
			},
			OnShutdown: func(ctx context.Context, err error) {
				shutdownCalls++
			},
		})

		h1 := pubsub.NewHandler(func(ctx context.Context, tp pubsub.Topic, m interface{}) error {
			return nil
		})

		topic := pubsub.Topic("test")
		s, err := broker.Subscribe(ctx, topic, h1)
		require.NoError(t, err)
		require.NotEmpty(t, s.ID())

		t.Run("WHEN publishing a message THEN publish hook is invoked", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, topic, "test"))
			require.EqualValues(t, 1, publishCalls)
		})

		t.Run("WHEN delivering a message THEN deliver hook is invoked", func(t *testing.T) {
			require.Eventually(t, func() bool {
				return deliverCalls == 1
			}, time.Second, 100*time.Millisecond)
		})

		t.Run("WHEN shutting down THEN shutdown hook is invoked", func(t *testing.T) {
			require.NoError(t, broker.Shutdown(ctx))
			require.EqualValues(t, 1, shutdownCalls)
		})
	})
}
