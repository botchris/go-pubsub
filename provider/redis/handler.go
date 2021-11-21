package redis

import (
	"context"

	"github.com/botchris/go-pubsub"
)

type subscription struct {
	ctx     context.Context
	cancel  context.CancelFunc
	handler pubsub.Handler
}
