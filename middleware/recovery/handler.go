package recovery

import (
	"context"

	"github.com/botchris/go-pubsub"
)

type handler struct {
	pubsub.Handler
	handler HandlerFunc
}

func (s *handler) Deliver(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
	defer func(ctx context.Context) {
		if p := recover(); p != nil {
			err = s.handler(ctx, p)
		}
	}(ctx)

	err = s.Handler.Deliver(ctx, topic, m)

	return
}
