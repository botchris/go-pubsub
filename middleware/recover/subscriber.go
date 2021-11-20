package recover

import (
	"context"

	"github.com/botchris/go-pubsub"
)

type subscriber struct {
	pubsub.Subscriber
	handler RecoveryHandlerFunc
}

func (s *subscriber) Deliver(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
	defer func(ctx context.Context) {
		if r := recover(); r != nil {
			err = recoverFrom(ctx, r, "pubsub: subscriber panic\n", s.handler)
		}
	}(ctx)

	err = s.Subscriber.Deliver(ctx, topic, m)

	return
}
