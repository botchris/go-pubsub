package codec

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/botchris/go-pubsub"
	lru "github.com/hashicorp/golang-lru"
)

// EncodeFunc is a function that encodes a message into a byte slice.
type EncodeFunc func(interface{}) ([]byte, error)

// DecodeFunc is a function that decodes a message from a byte slice.
type DecodeFunc func([]byte, interface{}) error

type middleware struct {
	pubsub.Broker
	cache *lru.Cache
	enc   EncodeFunc
	dec   DecodeFunc
}

// NewCodecMiddleware creates a new Codec middleware.
//
// Publishing:
// Intercepts each message and encodes it before publishing to underlying broker.
//
// Subscribers:
// Intercepts each message that is delivered to a subscribers and decodes it
// assuming that the incoming message is a byte slice.
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
func NewCodecMiddleware(broker pubsub.Broker, enc EncodeFunc, dec DecodeFunc) pubsub.Broker {
	cache, _ := lru.New(256)

	return &middleware{
		Broker: broker,
		cache:  cache,
		enc:    enc,
		dec:    dec,
	}
}

func (mw middleware) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	bytes, err := mw.enc(m)
	if err != nil {
		return err
	}

	return mw.Broker.Publish(ctx, topic, bytes)
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, subscriber pubsub.Subscriber) error {
	{
		rs := reflect.ValueOf(subscriber).Elem()
		rf := rs.FieldByName("messageType")
		rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()

		newMessageType := reflect.TypeOf([]byte{})
		rf.Set(reflect.ValueOf(newMessageType))
	}

	{
		rs := reflect.ValueOf(subscriber).Elem()
		rf := rs.FieldByName("messageKind")
		rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()

		newMessageKind := reflect.TypeOf(reflect.Slice)
		rf.Set(reflect.ValueOf(newMessageKind))
	}

	{
		rs := reflect.ValueOf(subscriber).Elem()
		rf := rs.FieldByName("handlerFunc")
		rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
		originalCallable := rf.Interface().(reflect.Value)

		handler := func(ctx context.Context, m interface{}) error {
			bytes, ok := m.([]byte)
			if !ok {
				return errors.New("message is not a []byte")
			}

			srf := subscriber.Reflect()
			h := sha1.New()
			key := append(bytes, []byte(srf.MessageType.String())...)
			hash := fmt.Sprintf("%x", h.Sum(key))

			if found, hit := mw.cache.Get(hash); hit {
				args := []reflect.Value{
					reflect.ValueOf(ctx),
					reflect.ValueOf(found),
				}

				if out := originalCallable.Call(args); out[0].Interface() != nil {
					return out[0].Interface().(error)
				}

				return nil
			}

			msg, err := mw.decodeFor(bytes, subscriber)
			if err != nil {
				return nil
			}

			mw.cache.Add(hash, msg)

			args := []reflect.Value{
				reflect.ValueOf(ctx),
				reflect.ValueOf(msg),
			}

			if out := originalCallable.Call(args); out[0].Interface() != nil {
				return out[0].Interface().(error)
			}

			return nil
		}

		newCallable := reflect.ValueOf(handler)
		rf.Set(reflect.ValueOf(newCallable))
	}

	return mw.Broker.Subscribe(ctx, topic, subscriber)
}

// decodeFor attempts to dynamically decode a raw message for provided
// subscriber using the given decoder function.
func (mw middleware) decodeFor(raw []byte, s pubsub.Subscriber) (interface{}, error) {
	srf := s.Reflect()
	base := srf.MessageType

	if srf.MessageKind == reflect.Ptr {
		base = base.Elem()
	}

	msg := reflect.New(base).Interface()
	if err := mw.dec(raw, msg); err != nil {
		return nil, err
	}

	if srf.MessageKind == reflect.Ptr {
		return msg, nil
	}

	return reflect.ValueOf(msg).Elem().Interface(), nil
}
