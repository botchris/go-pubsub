package codec

import (
	"context"

	"github.com/botchris/go-pubsub"
	lru "github.com/hashicorp/golang-lru"
)

// Codec represents a component that can encode and decode messages.
type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type middleware struct {
	pubsub.Broker
	cache *lru.Cache
	codec Codec
}

// NewCodecMiddleware creates a new Codec middleware that encodes/decodes
// messages when publishing and delivering.
//
// # When Publishing
//
// Intercepts each message being published and encodes it before passing it to
// wrapped broker.
//
// # When Delivering
//
// Intercepts each message before it gets delivered to subscriber handlers and
// decodes into handler's accepted type assuming that the incoming message is a
// byte slice (`[]byte`)
//
// Decoder function is invoked once for each destination type, and it takes
// two arguments:
//
// 1. The raw message as a byte slice
// 2. A pointer to a variable of type of handler's desired type.
//
// For example, given the following handler functions:
//
//     S1: func (ctx context.Context, msg string) error
//     S2: func (ctx context.Context, msg string) error
//     S3: func (ctx context.Context, msg map[string]string) error
//
// Then, decoder function will be invoked twice, once for each type:
//
// - `string`
// - `map[string]string`
//
// If decoder is unable to convert the given byte slice into the desired type
// (string or map in the above example), an error must be returned by Codec
// implementations. This will prevent from delivering the message to underlying
// handler.
//
// IMPORTANT:
//
// Message decoding are expensive operations as interception takes place each
// time a message is delivered to handler function. This may produce unnecessary
// decoding operation when the same message is delivered to multiple subscriptions.
// To address this issue, this middleware uses a small in-memory LRU cache of each
// seen decoded message to prevent from decoding the same message multiple times.
func NewCodecMiddleware(broker pubsub.Broker, codec Codec) pubsub.Broker {
	cache, _ := lru.New(256)

	return &middleware{
		Broker: broker,
		cache:  cache,
		codec:  codec,
	}
}

func (mw middleware) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	bytes, err := mw.codec.Encode(m)
	if err != nil {
		return err
	}

	return mw.Broker.Publish(ctx, topic, bytes)
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, h pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	nh := &handler{
		Handler: h,
		codec:   mw.codec,
		cache:   mw.cache,
	}

	return mw.Broker.Subscribe(ctx, topic, nh, option...)
}
