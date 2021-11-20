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

type subscriber struct {
	pubsub.Subscriber
	codec Codec
	cache *lru.Cache
}

func (s *subscriber) Deliver(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	bytes, ok := m.([]byte)
	if !ok {
		return errors.New("message is not a []byte")
	}

	srf := s.Subscriber.Reflect()

	h := sha1.New()
	key := append(bytes, []byte(srf.MessageType.String())...)
	hash := fmt.Sprintf("%x", h.Sum(key))

	if found, hit := s.cache.Get(hash); hit {
		return s.Subscriber.Deliver(ctx, topic, found)
	}

	msg, err := s.decodeFor(bytes, srf.MessageType, srf.MessageKind)
	if err != nil {
		return nil
	}

	s.cache.Add(hash, msg)

	return s.Subscriber.Deliver(ctx, topic, msg)
}

// decodeFor attempts to dynamically decode a raw message for provided
// subscriber using the given decoder function.
func (s *subscriber) decodeFor(raw []byte, mType reflect.Type, mKind reflect.Kind) (interface{}, error) {
	base := mType

	if mKind == reflect.Ptr {
		base = base.Elem()
	}

	msg := reflect.New(base).Interface()
	if err := s.codec.Decode(raw, msg); err != nil {
		return nil, err
	}

	if mKind == reflect.Ptr {
		return msg, nil
	}

	return reflect.ValueOf(msg).Elem().Interface(), nil
}
