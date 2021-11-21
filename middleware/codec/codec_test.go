package codec_test

import (
	"context"
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
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = codec.NewCodecMiddleware(broker, codec.Protobuf)

		var received *timestamppb.Timestamp
		now := timestamppb.Now()
		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, timestamp *timestamppb.Timestamp) error {
			received = timestamp

			return nil
		})

		_, err := broker.Subscribe(ctx, "test", h1)
		require.NoError(t, err)

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
		jcodec := &jsonCodecSpy{}
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = codec.NewCodecMiddleware(broker, jcodec)

		toSend := &testMessage{Value: "test"}

		var rcv1 *testMessage
		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, msg *testMessage) error {
			rcv1 = msg

			return nil
		})

		var rcv2 *testMessage
		h2 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, msg *testMessage) error {
			rcv2 = msg

			return nil
		})

		_, err := broker.Subscribe(ctx, "test", h1)
		_, err = broker.Subscribe(ctx, "test", h2)

		require.NoError(t, err)
		require.NoError(t, err)

		t.Run("WHEN publishing a custom pointer message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder is invoked only once", func(t *testing.T) {
				require.NotEmpty(t, rcv1)
				require.NotEmpty(t, rcv2)
				require.EqualValues(t, jcodec.decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with one subscribers and JSON encoder/decoder", func(t *testing.T) {
		jcodec := &jsonCodecSpy{}
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = codec.NewCodecMiddleware(broker, jcodec)
		toSend := testMessage{Value: "test"}

		var rcv testMessage
		h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, msg testMessage) error {
			rcv = msg

			return nil
		})

		_, err := broker.Subscribe(ctx, "test", h1)
		require.NoError(t, err)

		t.Run("WHEN publishing a custom message by value", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, jcodec.decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with one subscribers and JSON encoder/decoder", func(t *testing.T) {
		jcodec := &jsonCodecSpy{}
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = codec.NewCodecMiddleware(broker, jcodec)
		toSend := map[string]string{"value": "test"}

		var rcv map[string]string
		h := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, msg map[string]string) error {
			rcv = msg

			return nil
		})

		_, err := broker.Subscribe(ctx, "test", h)
		require.NoError(t, err)

		t.Run("WHEN publishing a map<string,string> message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, jcodec.decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with one subscribers and JSON encoder/decoder", func(t *testing.T) {
		jcodec := &jsonCodecSpy{}
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = codec.NewCodecMiddleware(broker, jcodec)
		toSend := "testing"

		var rcv string
		h := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, msg string) error {
			rcv = msg

			return nil
		})

		_, err := broker.Subscribe(ctx, "test", h)
		require.NoError(t, err)

		t.Run("WHEN publishing a string message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, "test", toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, jcodec.decoderCalls, 1)
			})
		})
	})

	t.Run("GIVEN a memory broker with a catch-all subscribers and JSON encoder/decoder", func(t *testing.T) {
		jcodec := &jsonCodecSpy{}
		broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)
		broker = codec.NewCodecMiddleware(broker, jcodec)
		toSend := "testing"
		topic := pubsub.Topic("test")

		var rcv string
		h := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, msg interface{}) error {
			rcv = msg.(string)

			return nil
		})

		_, err := broker.Subscribe(ctx, topic, h)
		require.NoError(t, err)

		t.Run("WHEN publishing a string message", func(t *testing.T) {
			require.NoError(t, broker.Publish(ctx, topic, toSend))

			t.Run("THEN subscribers receives the message and decoder function is invoked", func(t *testing.T) {
				require.NotEmpty(t, rcv)
				require.EqualValues(t, toSend, rcv)
				require.EqualValues(t, jcodec.decoderCalls, 1)
			})
		})
	})
}

type testMessage struct {
	Value string
}

type jsonCodecSpy struct {
	encoderCalls int
	decoderCalls int
}

func (j *jsonCodecSpy) Encode(i interface{}) ([]byte, error) {
	j.encoderCalls++

	return codec.JSON.Encode(i)
}

func (j *jsonCodecSpy) Decode(bytes []byte, i interface{}) error {
	j.decoderCalls++

	return codec.JSON.Decode(bytes, i)
}
