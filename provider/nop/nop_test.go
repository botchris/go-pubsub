package nop_test

import (
	"context"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/nop"
	"github.com/stretchr/testify/require"
)

func TestBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("GIVEN a nop broker", func(t *testing.T) {
		broker := nop.NewBroker()
		require.NotNil(t, broker)

		t.Run("WHEN publishing a message THEN no error are raised", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", "testing"))
		})
	})

	t.Run("GIVEN a nop broker and a subscriber", func(t *testing.T) {
		broker := nop.NewBroker()
		require.NotNil(t, broker)

		sub := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m string) error {
			return nil
		})

		t.Run("WHEN adding a subscriber", func(t *testing.T) {
			require.NoError(t, broker.Subscribe(ctx, "testing", sub))

			t.Run("THEN broker has no subscriptions", func(t *testing.T) {
				subs, err := broker.Subscriptions(ctx)
				require.NoError(t, err)
				require.Empty(t, subs)
			})
		})
	})
}
