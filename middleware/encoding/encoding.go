package encoding

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"

	"github.com/botchris/go-pubsub"
	lru "github.com/hashicorp/golang-lru"
)

// EncodeFunc is a function that encodes a message into a byte slice.
type EncodeFunc func(interface{}) ([]byte, error)

// DecodeFunc is a function that decodes a message from a byte slice.
type DecodeFunc func([]byte) (interface{}, error)

// PublishInterceptor intercepts each message and encodes it before publishing.
func PublishInterceptor(encoder EncodeFunc) pubsub.PublishInterceptor {
	return func(ctx context.Context, next pubsub.PublishHandler) pubsub.PublishHandler {
		return func(ctx context.Context, topic pubsub.Topic, m interface{}) error {
			pb, err := encoder(m)
			if err != nil {
				return err
			}

			return next(ctx, topic, pb)
		}
	}
}

// SubscriberInterceptor intercepts each message that is delivered to a
// subscribers and decodes it assuming the message is a byte slice.
//
// Interceptors are applied each time a message is delivered to a subscriber.
// This causes unnecessary decoding operation for the same message. To solve
// this problem, this interceptor can use a small LRU cache of each seen message
// to keep track of decoded messages.
func SubscriberInterceptor(decoder DecodeFunc, useCache bool) pubsub.SubscriberInterceptor {
	var cache *lru.Cache

	if useCache {
		cache, _ = lru.New(256)
	}

	return func(ctx context.Context, next pubsub.SubscriberMessageHandler) pubsub.SubscriberMessageHandler {
		return func(ctx context.Context, s *pubsub.Subscriber, t pubsub.Topic, m interface{}) error {
			bytes, ok := m.([]byte)
			if !ok {
				return errors.New("message is not a []byte")
			}

			if cache != nil {
				h := sha1.New()
				hash := fmt.Sprintf("% x", h.Sum(bytes))

				if found, ok := cache.Get(hash); ok {
					return next(ctx, s, t, found)
				}

				msg, err := decoder(bytes)
				if err != nil {
					return err
				}

				cache.Add(hash, msg)

				return next(ctx, s, t, msg)
			}

			msg, err := decoder(bytes)
			if err != nil {
				return err
			}

			return next(ctx, s, t, msg)
		}
	}
}
