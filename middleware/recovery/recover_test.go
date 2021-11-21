package recovery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/recovery"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
)

func TestPublishInterceptor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("GIVEN a broker with publish recovery middleware", func(t *testing.T) {
		broker := pubsub.Broker(&panicBroker{})

		recoveryErr := errors.New("recovery function")
		rec := func(ctx context.Context, p interface{}) error {
			return recoveryErr
		}

		broker = recovery.NewRecoveryMiddleware(broker, rec)

		t.Run("WHEN publish panics", func(t *testing.T) {
			var err error

			require.NotPanics(t, func() {
				err = broker.Publish(ctx, "test", "test")
			})

			t.Run("THEN publish recover from panics", func(t *testing.T) {
				require.Error(t, err)
				require.ErrorIs(t, err, recoveryErr)
			})
		})
	})
}

func TestSubscribeInterceptor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("GIVEN a in-memory broker with one subscriber", func(t *testing.T) {
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)

		recoveryCalls := 0
		rec := func(ctx context.Context, p interface{}) error {
			recoveryCalls++

			return errors.New("recovery function")
		}

		broker = recovery.NewRecoveryMiddleware(broker, rec)
		subCalls := 0

		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, p string) error {
			subCalls++

			panic("subscriber panic")
		})

		_, err := broker.Subscribe(ctx, "test", h1)
		require.NoError(t, err)

		t.Run("WHEN subscriber panics", func(t *testing.T) {
			var err error

			require.NotPanics(t, func() {
				err = broker.Publish(ctx, "test", "test")
			})

			t.Run("THEN publish recover from panics", func(t *testing.T) {
				require.NoError(t, err)
				require.NotZero(t, subCalls)
				require.NotZero(t, recoveryCalls)
			})
		})
	})
}

type panicBroker struct {
	pubsub.Broker
}

func (p *panicBroker) Publish(_ context.Context, _ pubsub.Topic, _ interface{}) error {
	panic("implement me")
}
