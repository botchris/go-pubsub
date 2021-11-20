package printer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/botchris/go-pubsub"
)

type middleware struct {
	pubsub.Broker
	writer io.Writer
}

// NewPrinterMiddleware returns a new middleware that prints messages
// to the writer the given write when publishing or delivering messages
func NewPrinterMiddleware(broker pubsub.Broker, writer io.Writer) pubsub.Broker {
	return &middleware{
		Broker: broker,
		writer: writer,
	}
}

func (mw middleware) Publish(ctx context.Context, topic pubsub.Topic, msg interface{}) error {
	j, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	log := fmt.Sprintf("[middleware] publishing @ %s: %s\n", topic, string(j))
	if _, err := mw.writer.Write([]byte(log)); err != nil {
		return err
	}

	return mw.Broker.Publish(ctx, topic, msg)
}

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, subscriber pubsub.Subscriber) error {
	rs := reflect.ValueOf(subscriber).Elem()
	rf := rs.FieldByName("handlerFunc")
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	originalCallable := rf.Interface().(reflect.Value)

	handler := func(ctx context.Context, t pubsub.Topic, m interface{}) error {
		j, err := json.Marshal(m)
		if err != nil {
			return err
		}

		log := fmt.Sprintf("[middleware] received @ %s : %s\n", topic, string(j))
		if _, err := mw.writer.Write([]byte(log)); err != nil {
			return err
		}

		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(t),
			reflect.ValueOf(m),
		}

		if out := originalCallable.Call(args); out[0].Interface() != nil {
			return out[0].Interface().(error)
		}

		return nil
	}

	newCallable := reflect.ValueOf(handler)
	rf.Set(reflect.ValueOf(newCallable))

	return mw.Broker.Subscribe(ctx, topic, subscriber)
}
