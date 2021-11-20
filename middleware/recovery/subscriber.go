package recovery

import (
	"context"

	"github.com/botchris/go-pubsub"
)

type subscriber struct {
	pubsub.Subscriber
	handler HandlerFunc
}

func (s *subscriber) Deliver(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
	defer func(ctx context.Context) {
		if p := recover(); p != nil {
			err = s.handler(ctx, p)
		}
	}(ctx)

	err = s.Subscriber.Deliver(ctx, topic, m)

	return
}
