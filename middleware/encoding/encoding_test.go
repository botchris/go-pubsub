package encoding_test

import (
	"context"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/encoding"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestInterceptors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a memory broker with one subscriber and proto encoder/decoder interceptors", func(t *testing.T) {
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = pubsub.NewMiddlewareBroker(broker,
			pubsub.WithPublishInterceptor(
				encoding.PublishInterceptor(encoding.ProtoEncoder),
			),
			pubsub.WithSubscriberInterceptor(
				encoding.SubscriberInterceptor(encoding.ProtoDecoder, true),
			),
		)

		calls := 0
		sub1 := pubsub.NewSubscriber(func(ctx context.Context, timestamp *timestamppb.Timestamp) error {
			calls++

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub1))

		t.Run("WHEN publishing a proto message THEN subscriber receives the proto", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", timestamppb.Now()))
			require.EqualValues(t, calls, 1)
		})
	})

	t.Run("GIVEN a memory broker with two subscribers and custom encoder/decoder with cache capabilities", func(t *testing.T) {
		decoderCalls := 0
		decoder := func(bytes []byte) (interface{}, error) {
			decoderCalls++

			return "custom", nil
		}

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = pubsub.NewMiddlewareBroker(broker,
			pubsub.WithPublishInterceptor(
				encoding.PublishInterceptor(func(i interface{}) ([]byte, error) {
					return []byte("custom"), nil
				}),
			),
			pubsub.WithSubscriberInterceptor(
				encoding.SubscriberInterceptor(decoder, true),
			),
		)

		sub1Calls := 0
		sub1 := pubsub.NewSubscriber(func(ctx context.Context, msg string) error {
			sub1Calls++

			return nil
		})

		sub2Calls := 0
		sub2 := pubsub.NewSubscriber(func(ctx context.Context, msg string) error {
			sub2Calls++

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub1))
		require.NoError(t, broker.Subscribe(ctx, "test", sub2))

		t.Run("WHEN publishing a string message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", "custom"))

			t.Run("THEN subscribers receives the message and decoder function is invoked only once", func(t *testing.T) {
				require.EqualValues(t, sub1Calls, 1)
				require.EqualValues(t, sub2Calls, 1)
				require.EqualValues(t, decoderCalls, 1)
			})
		})
	})
}
