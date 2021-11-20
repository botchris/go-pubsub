package codec

import (
	"context"

	"github.com/botchris/go-pubsub"
)

type subscriber struct {
	parent pubsub.Subscriber
}

func (s subscriber) ID() string {
	return s.parent.ID()
}

func (s subscriber) Accepts(msg interface{}) bool {
	return s.parent.Accepts(msg)
}

func (s subscriber) Deliver(ctx context.Context, topic pubsub.Topic, message interface{}) error {
	return s.parent.Deliver(ctx, topic, message)
}

func (s subscriber) String() string {
	return s.parent.String()
}
