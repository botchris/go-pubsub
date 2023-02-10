package codec

import (
	"context"
	"fmt"
	"reflect"

	"github.com/botchris/go-pubsub"
	lru "github.com/hashicorp/golang-lru"
	"github.com/segmentio/fasthash/fnv1a"
)

type handler struct {
	pubsub.Handler
	cache *lru.Cache
	codec Codec
}

func (s *handler) Deliver(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	bytes, ok := m.([]byte)
	if !ok {
		return fmt.Errorf("delivery failure: expecting message to be of type []byte, but got `%T`", m)
	}

	srf := s.Handler.Reflect()
	cacheKey := fmt.Sprintf("%s:%d", srf.MessageType.String(), fnv1a.HashBytes64(bytes))

	if found, hit := s.cache.Get(cacheKey); hit {
		return s.Handler.Deliver(ctx, topic, found)
	}

	msg, err := s.decodeFor(bytes, srf.MessageType, srf.MessageKind)
	if err != nil {
		return nil
	}

	s.cache.Add(cacheKey, msg)

	return s.Handler.Deliver(ctx, topic, msg)
}

func (s *handler) decodeFor(raw []byte, mType reflect.Type, mKind reflect.Kind) (interface{}, error) {
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
