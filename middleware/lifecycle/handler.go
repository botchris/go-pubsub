package lifecycle

import (
	"context"

	"github.com/botchris/go-pubsub"
)

type handler struct {
	pubsub.Handler
	hook func(ctx context.Context, topic pubsub.Topic, m interface{}, err error)
}

func (h *handler) Deliver(ctx context.Context, topic pubsub.Topic, message interface{}) error {
	err := h.Handler.Deliver(ctx, topic, message)
	if h.hook != nil {
		h.hook(ctx, topic, message, err)
	}

	return err
}
