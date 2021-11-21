package memory

import (
	"github.com/botchris/go-pubsub"
)

type subscription struct {
	id      string
	topic   pubsub.Topic
	options pubsub.SubscribeOptions
	unsub   func() error
	handler pubsub.Handler
}

func (s *subscription) ID() string {
	return s.id
}

func (s *subscription) Options() pubsub.SubscribeOptions {
	return s.options
}

func (s *subscription) Topic() pubsub.Topic {
	return s.topic
}

func (s *subscription) Unsubscribe() error {
	return s.unsub()
}

func (s *subscription) Handler() pubsub.Handler {
	return s.handler
}
