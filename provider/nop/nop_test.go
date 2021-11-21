package nop_test

import (
	"context"
	"testing"
	"time"

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
}
