package printer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/botchris/go-pubsub"
)

type middleware struct {
	pubsub.Broker
	writer io.Writer
}

// NewPrinterMiddleware returns a new middleware that prints messages
// to the writer the given write when publishing or delivering messages.
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

func (mw middleware) Subscribe(ctx context.Context, topic pubsub.Topic, sub pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	s := &handler{
		Handler: sub,
		writer:  mw.writer,
	}

	return mw.Broker.Subscribe(ctx, topic, s, option...)
}
