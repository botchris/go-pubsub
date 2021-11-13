package kmq

import (
	"context"
	"time"
)

// Option defines a function signature for configuration options.
type Option interface {
	apply(*options)
}

type options struct {
	ctx            context.Context
	serverHost     string
	serverPort     int
	clientID       string
	groupID        string
	encoder        Encoder
	decoder        Decoder
	deliverTimeout time.Duration
}

type fnOption struct {
	f func(*options)
}

func (f fnOption) apply(o *options) {
	f.f(o)
}

// WithContext sets the context to be used by broker's runner
func WithContext(ctx context.Context) Option {
	return fnOption{
		f: func(o *options) {
			o.ctx = ctx
		},
	}
}

func WithServerHost(host string) Option {
	return fnOption{
		f: func(o *options) {
			o.serverHost = host
		},
	}
}

func WithServerPort(serverPort int) Option {
	return fnOption{
		f: func(o *options) {
			o.serverPort = serverPort
		},
	}
}

// WithClientID sets the client ID to be used when registering to KubeMQ server.
func WithClientID(clientID string) Option {
	return fnOption{
		f: func(o *options) {
			o.clientID = clientID
		},
	}
}

// WithGroupID sets the group ID for receiving messages.
// Subscriptions under the same groupID share the messages in a round-robin fashion.
func WithGroupID(groupID string) Option {
	return fnOption{
		f: func(o *options) {
			o.groupID = groupID
		},
	}
}

// WithEncoder sets the encoder function to be used by broker.
// Use this to define how messages are encoded before sending to the SNS service.
func WithEncoder(e Encoder) Option {
	return fnOption{
		f: func(o *options) {
			o.encoder = e
		},
	}
}

// WithDecoder sets the decoder function to be used by broker.
// Use this to define how messages are decoded after receiving from the SQS service.
func WithDecoder(d Decoder) Option {
	return fnOption{
		f: func(o *options) {
			o.decoder = d
		},
	}
}

// WithDeliveryTimeout sets the max execution time a subscriber has to handle a message.
func WithDeliveryTimeout(t time.Duration) Option {
	return fnOption{
		f: func(o *options) {
			o.deliverTimeout = t
		},
	}
}
