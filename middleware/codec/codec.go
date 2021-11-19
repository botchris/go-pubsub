package codec

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"reflect"

	"github.com/botchris/go-pubsub"
	lru "github.com/hashicorp/golang-lru"
)

// EncodeFunc is a function that encodes a message into a byte slice.
type EncodeFunc func(interface{}) ([]byte, error)

// DecodeFunc is a function that decodes a message from a byte slice.
type DecodeFunc func([]byte, interface{}) error

// PublishInterceptor intercepts each message and encodes it before publishing.
func PublishInterceptor(encoder EncodeFunc) pubsub.PublishInterceptor {
	return func(ctx context.Context, next pubsub.PublishHandler) pubsub.PublishHandler {
		return func(ctx context.Context, topic pubsub.Topic, m interface{}) error {
			enc, err := encoder(m)
			if err != nil {
				return err
			}

			return next(ctx, topic, enc)
		}
	}
}

// SubscriberInterceptor intercepts each message that is delivered to a
// subscribers and decodes it assuming that the message is a byte slice.
//
// Decoder function is invoked once for each desired type, and it takes
// two arguments:
//
// 1. The raw message as a byte slice
// 2. A pointer to a variable of type of subscriber's desired type.
//
// For example, given the following subscribers:
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
// If decoder is unable to convert the given byte slice into the desired type,
// and error must be returned. This will prevent from delivering the message to
// underlying subscriber.
//
// NOTE: message decoding are expensive operations.
// In the other hand, interceptors are applied each time a message is delivered
// to subscribers. This may produce unnecessary decoding operation when the same
// message is delivered to multiple subscribers. To address this issue, this
// interceptor uses a small LRU cache of each seen decoded message.
func SubscriberInterceptor(decoder DecodeFunc) pubsub.SubscriberInterceptor {
	cache, _ := lru.New(256)

	return func(ctx context.Context, next pubsub.SubscriberMessageHandler) pubsub.SubscriberMessageHandler {
		return func(ctx context.Context, s *pubsub.Subscriber, t pubsub.Topic, m interface{}) error {
			bytes, ok := m.([]byte)
			if !ok {
				return errors.New("message is not a []byte")
			}

			srf := s.Reflect()

			h := sha1.New()
			key := append(bytes, []byte(srf.MessageType.String())...)
			hash := fmt.Sprintf("%x", h.Sum(key))

			if found, hit := cache.Get(hash); hit {
				return next(ctx, s, t, found)
			}

			base := srf.MessageType
			if srf.MessageKind == reflect.Ptr {
				base = base.Elem()
			}

			msg := reflect.New(base).Interface()
			if err := decoder(bytes, msg); err != nil {
				return err
			}

			if srf.MessageKind == reflect.Ptr {
				cache.Add(hash, msg)

				return next(ctx, s, t, msg)
			}

			message := reflect.ValueOf(msg).Elem().Interface()
			cache.Add(hash, message)

			return next(ctx, s, t, message)
		}
	}
}
