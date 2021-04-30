package pubsub_test

import (
	"context"
	"testing"

	"github.com/ChristopherCastro/go-pubsub"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_Subscriber(t *testing.T) {
	tests := []struct {
		name        string
		handlerFunc interface{}
		panic       error
		acceptsAny  bool
	}{
		{
			name:        "compound proto.Message (any)",
			handlerFunc: func(ctx context.Context, m CompoundProtoMessageInterface) error { return nil },
			panic:       nil,
			acceptsAny:  false,
		},
		{
			name:        "message by value",
			handlerFunc: func(ctx context.Context, m emptypb.Empty) error { return nil },
			panic:       pubsub.ErrSubscriberInputInvalidKind,
		},
		{
			name:        "message by pointer",
			handlerFunc: func(ctx context.Context, m *emptypb.Empty) error { return nil },
			panic:       nil,
		},
		{
			name:        "anything by proto.Message",
			handlerFunc: func(ctx context.Context, m proto.Message) error { return nil },
			panic:       nil,
			acceptsAny:  true,
		},
		{
			name:        "pointer to compound proto",
			handlerFunc: func(ctx context.Context, m *CompoundProtoMessage) error { return nil },
			panic:       nil,
		},
		{
			name:        "invalid context",
			handlerFunc: func(ctx interface{}, m *CompoundProtoMessage) error { return nil },
			panic:       pubsub.ErrSubscriberInputNoContext,
		},
		{
			name:        "no return",
			handlerFunc: func(ctx context.Context, m *CompoundProtoMessage) {},
			panic:       pubsub.ErrSubscriberOutputLengthMissMatch,
		},
		{
			name:        "returns not of error kind",
			handlerFunc: func(ctx context.Context, m *CompoundProtoMessage) *CompoundProtoMessage { return nil },
			panic:       pubsub.ErrSubscriberOutputNoError,
		},
		{
			name:        "returns compound error interface",
			handlerFunc: func(ctx context.Context, m *CompoundProtoMessage) CompoundErrorInterface { return nil },
			panic:       nil,
		},
		{
			name:        "returns pointer to compound error interface",
			handlerFunc: func(ctx context.Context, m *CompoundProtoMessage) *CompoundError { return nil },
			panic:       nil,
		},
		{
			name:        "returns value of compound error interface",
			handlerFunc: func(ctx context.Context, m *CompoundProtoMessage) CompoundError { return CompoundError{} },
			panic:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := func() {
				name := pubsub.NewSubscriber(tt.handlerFunc).String()

				if tt.acceptsAny {
					require.Contains(t, name, "(*)")
				} else {
					require.NotContains(t, name, "(*)")
				}
			}

			if tt.panic != nil {
				require.PanicsWithError(t, tt.panic.Error(), operation)
			} else {
				require.NotPanics(t, operation)
			}
		})
	}
}

type CompoundProtoMessageInterface interface {
	proto.Message
}

type CompoundProto struct {
	CompoundProtoMessageInterface
}

type CompoundErrorInterface interface {
	error
}

type CompoundError struct {
	CompoundErrorInterface
}

type CompoundProtoMessage struct {
	emptypb.Empty
}
