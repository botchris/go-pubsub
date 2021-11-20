package recover

import (
	"context"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/botchris/go-pubsub"
)

// RecoveryHandlerFunc is a function that recovers from the panic `p` by returning an `error`.
type RecoveryHandlerFunc func(ctx context.Context, p interface{}) error

type middleware struct {
	pubsub.Broker
	handler RecoveryHandlerFunc
}

// NewRecoveryMiddleware returns a new middleware that recovers from panics when
// publishing to topics or delivering to subscribers.
func NewRecoveryMiddleware(parent pubsub.Broker, handler RecoveryHandlerFunc) pubsub.Broker {
	return &middleware{
		Broker:  parent,
		handler: handler,
	}
}

func (mw middleware) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
	defer func(ctx context.Context) {
		if r := recover(); r != nil {
			err = recoverFrom(ctx, r, "pubsub: publisher panic\n", mw.handler)
		}
	}(ctx)

	err = mw.Broker.Publish(ctx, topic, m)

	return
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	rs := reflect.ValueOf(subscriber).Elem()
	rf := rs.FieldByName("handlerFunc")
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	originalCallable := rf.Interface().(reflect.Value)

	handler := func(ctx context.Context, t pubsub.Topic, m interface{}) (err error) {
		defer func(ctx context.Context) {
			if r := recover(); r != nil {
				err = recoverFrom(ctx, r, "pubsub: subscriber panic\n", mw.handler)
			}
		}(ctx)

		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(t),
			reflect.ValueOf(m),
		}

		if out := originalCallable.Call(args); out[0].Interface() != nil {
			err = out[0].Interface().(error)
		}

		return
	}

	newCallable := reflect.ValueOf(handler)
	rf.Set(reflect.ValueOf(newCallable))

	return mw.Broker.Subscribe(ctx, topic, subscriber)
}

func (mw middleware) Unsubscribe(ctx context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	return mw.Broker.Unsubscribe(ctx, topic, subscriber)
}

func recoverFrom(ctx context.Context, p interface{}, wrap string, r RecoveryHandlerFunc) error {
	if r == nil {
		return fmt.Errorf("%s: %s", p, wrap)
	}

	return r(ctx, p)
}
