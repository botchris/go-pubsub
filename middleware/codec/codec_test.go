package codec_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/codec"
	"github.com/botchris/go-pubsub/provider/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProto(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a memory broker with one subscriber and proto encoder/decoder interceptors", func(t *testing.T) {
		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = codec.NewCodecMiddleware(broker, codec.ProtoEncoder, codec.ProtoDecoder)

		var received *timestamppb.Timestamp
		now := timestamppb.Now()
		sub1 := pubsub.NewSubscriber(func(ctx context.Context, timestamp *timestamppb.Timestamp) error {
			received = timestamp

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub1))

		t.Run("WHEN publishing a proto message THEN subscriber receives the proto", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", now))
			require.NotNil(t, received)
			require.EqualValues(t, now.AsTime().Unix(), received.AsTime().Unix())
		})
	})
}

func TestJson(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a memory broker with two subscribers and JSON encoder/decoder", func(t *testing.T) {
		decoderCalls := 0
		decoder := func(data []byte, v interface{}) error {
			decoderCalls++

			return json.Unmarshal(data, &v)
		}

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = codec.NewCodecMiddleware(broker, json.Marshal, decoder)

		toSend := &testMessage{Value: "test"}

		var rcv1 *testMessage
		sub1 := pubsub.NewSubscriber(func(ctx context.Context, msg *testMessage) error {
			rcv1 = msg

			return nil
		})

		var rcv2 *testMessage
		sub2 := pubsub.NewSubscriber(func(ctx context.Context, msg *testMessage) error {
			rcv2 = msg

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub1))
		require.NoError(t, broker.Subscribe(ctx, "test", sub2))

		t.Run("WHEN publishing a custom pointer message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked only once", func(t *testing.T) {
				require.NotEmpty(t, rcv1)
				require.NotEmpty(t, rcv2)
				require.EqualValues(t, decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with one subscribers and JSON encoder/decoder", func(t *testing.T) {
		decoderCalls := 0
		decoder := func(data []byte, v interface{}) error {
			decoderCalls++

			return json.Unmarshal(data, &v)
		}

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = codec.NewCodecMiddleware(broker, json.Marshal, decoder)
		toSend := testMessage{Value: "test"}

		var rcv testMessage
		sub := pubsub.NewSubscriber(func(ctx context.Context, msg testMessage) error {
			rcv = msg

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub))

		t.Run("WHEN publishing a custom message by value", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with one subscribers and JSON encoder/decoder", func(t *testing.T) {
		decoderCalls := 0
		decoder := func(data []byte, v interface{}) error {
			decoderCalls++

			return json.Unmarshal(data, &v)
		}

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = codec.NewCodecMiddleware(broker, json.Marshal, decoder)
		toSend := map[string]string{"value": "test"}

		var rcv map[string]string
		sub := pubsub.NewSubscriber(func(ctx context.Context, msg map[string]string) error {
			rcv = msg

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub))

		t.Run("WHEN publishing a map<string,string> message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with one subscribers and JSON encoder/decoder", func(t *testing.T) {
		decoderCalls := 0
		decoder := func(data []byte, v interface{}) error {
			decoderCalls++

			return json.Unmarshal(data, &v)
		}

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = codec.NewCodecMiddleware(broker, json.Marshal, decoder)
		toSend := "testing"

		var rcv string
		sub := pubsub.NewSubscriber(func(ctx context.Context, msg string) error {
			rcv = msg

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub))

		t.Run("WHEN publishing a string message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with a catch-all subscribers and JSON encoder/decoder", func(t *testing.T) {
		decoderCalls := 0
		decoder := func(data []byte, v interface{}) error {
			decoderCalls++

			return json.Unmarshal(data, &v)
		}

		broker := memory.NewBroker(memory.NopSubscriberErrorHandler)
		broker = codec.NewCodecMiddleware(broker, json.Marshal, decoder)
		toSend := "testing"

		var rcv string
		sub := pubsub.NewSubscriber(func(ctx context.Context, msg interface{}) error {
			rcv = msg.(string)

			return nil
		})

		require.NoError(t, broker.Subscribe(ctx, "test", sub))

		t.Run("WHEN publishing a string message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, decoderCalls, 1)
			})
		})
	})
}

type testMessage struct {
	Value string
}
