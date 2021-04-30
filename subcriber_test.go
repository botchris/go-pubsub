package pubsub_test

import (
	"context"
	"testing"

	"github.com/ChristopherCastro/go-pubsub"
	"github.com/stretchr/testify/require"
)

func Test_Subscriber(t *testing.T) {
	tests := []struct {
		name        string
		handlerFunc interface{}
		panic       error
	}{
		{
			name:        "compound interface",
			handlerFunc: func(ctx context.Context, m CompoundProtoMessageInterface) error { return nil },
			panic:       nil,
		},
		{
			name:        "message by value",
			handlerFunc: func(ctx context.Context, m EmptyMessage) error { return nil },
			panic:       nil,
		},
		{
			name:        "message by pointer",
			handlerFunc: func(ctx context.Context, m *EmptyMessage) error { return nil },
			panic:       nil,
		},
		{
			name:        "anything of custom interface",
			handlerFunc: func(ctx context.Context, m CustomInterface) error { return nil },
			panic:       nil,
		},
		{
			name:        "pointer to compound message",
			handlerFunc: func(ctx context.Context, m *CompoundMessage) error { return nil },
			panic:       nil,
		},
		{
			name:        "invalid context",
			handlerFunc: func(ctx interface{}, m *CompoundMessage) error { return nil },
			panic:       pubsub.ErrSubscriberInputNoContext,
		},
		{
			name:        "no return",
			handlerFunc: func(ctx context.Context, m *CompoundMessage) {},
			panic:       pubsub.ErrSubscriberOutputLengthMissMatch,
		},
		{
			name:        "returns not of error kind",
			handlerFunc: func(ctx context.Context, m *CompoundMessage) *CompoundMessage { return nil },
			panic:       pubsub.ErrSubscriberOutputNoError,
		},
		{
			name:        "returns compound error interface",
			handlerFunc: func(ctx context.Context, m *CompoundMessage) CompoundErrorInterface { return nil },
			panic:       nil,
		},
		{
			name:        "returns pointer to compound error interface",
			handlerFunc: func(ctx context.Context, m *CompoundMessage) *CompoundError { return nil },
			panic:       nil,
		},
		{
			name:        "returns value of compound error interface",
			handlerFunc: func(ctx context.Context, m *CompoundMessage) CompoundError { return CompoundError{} },
			panic:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := func() {
				pubsub.NewSubscriber(tt.handlerFunc)
			}

			if tt.panic != nil {
				require.PanicsWithError(t, tt.panic.Error(), operation)
			} else {
				require.NotPanics(t, operation)
			}
		})
	}
}

type CustomInterface interface {
	private()
}

type CompoundProtoMessageInterface interface {
	CustomInterface
}

type CompoundErrorInterface interface {
	error
}

type CompoundError struct {
	CompoundErrorInterface
}

type EmptyMessage struct {
}

type CompoundMessage struct {
	EmptyMessage
}
