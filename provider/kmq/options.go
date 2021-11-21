package kmq

import (
	"time"
)

// Option defines a function signature for configuration options.
type Option interface {
	apply(*options)
}

type options struct {
	serverHost       string
	serverPort       int
	clientID         string
	onStreamError    func(error)
	onSubscribeError func(error)
	deliverTimeout   time.Duration
	autoReconnect    bool
}

type fnOption struct {
	f func(*options)
}

func (f fnOption) apply(o *options) {
	f.f(o)
}

// WithServerHost sets KubeMQ server host. e.g. `localhost`
func WithServerHost(host string) Option {
	return fnOption{
		f: func(o *options) {
			o.serverHost = host
		},
	}
}

// WithServerPort sets KubeMQ server grpc port. Default: 50000
func WithServerPort(serverPort int) Option {
	return fnOption{
		f: func(o *options) {
			o.serverPort = serverPort
		},
	}
}

// WithClientID sets the client ID to be used when registering to KubeMQ server.
// Required.
func WithClientID(clientID string) Option {
	return fnOption{
		f: func(o *options) {
			o.clientID = clientID
		},
	}
}

// WithStreamErrorHandler sets the function to be called when an error occurs
// when streaming events to KubeMQ (publish).
func WithStreamErrorHandler(f func(error)) Option {
	return fnOption{
		f: func(o *options) {
			o.onStreamError = f
		},
	}
}

// WithSubscribeErrorHandler sets the function to be called when an error occurs
// when receiving a message from KubeMQ.
func WithSubscribeErrorHandler(f func(error)) Option {
	return fnOption{
		f: func(o *options) {
			o.onSubscribeError = f
		},
	}
}

// WithDeliveryTimeout sets the max execution time a subscriber has to handle a
// message. Default: 5s
func WithDeliveryTimeout(t time.Duration) Option {
	return fnOption{
		f: func(o *options) {
			o.deliverTimeout = t
		},
	}
}

// WithAutoReconnect sets the auto reconnect flag. Default: true
func WithAutoReconnect(b bool) Option {
	return fnOption{
		f: func(o *options) {
			o.autoReconnect = b
		},
	}
}
