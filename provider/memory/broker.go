// Package memory provides a simple in-memory Broker.
//
// This broker should never be used for IPC purposes
// (Inter-Process Communication) as it only works by moving messages using
// local memory.
package memory

import (
	"context"
	"sync"

	"github.com/botchris/go-pubsub"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
)

// SubscriptionErrorHandler used to handle subscribers errors when delivering a
// message.
type SubscriptionErrorHandler func(ctx context.Context, topic pubsub.Topic, s pubsub.Subscription, m interface{}, err error)

// NopSubscriptionErrorHandler an empty error handler
var NopSubscriptionErrorHandler = func(ctx context.Context, topic pubsub.Topic, s pubsub.Subscription, m interface{}, err error) {}

type broker struct {
	topics        map[pubsub.Topic]*topic
	subErrHandler SubscriptionErrorHandler
	sync.RWMutex
}

// NewBroker returns a new in-memory broker instance.
func NewBroker(subErrHandler SubscriptionErrorHandler) pubsub.Broker {
	return &broker{
		topics:        make(map[pubsub.Topic]*topic),
		subErrHandler: subErrHandler,
	}
}

func (b *broker) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	b.Lock()
	t := b.openTopic(topic)
	b.Unlock()

	for _, result := range t.publish(ctx, m) {
		if result.err != nil {
			b.subErrHandler(ctx, t.id, result.subscriber, m, result.err)
		}
	}

	return nil
}

func (b *broker) Subscribe(_ context.Context, topic pubsub.Topic, handler pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	b.Lock()
	defer b.Unlock()

	opts := pubsub.NewSubscribeOptions()
	for _, o := range option {
		o(opts)
	}

	sid := uuid.New()
	sub := &subscription{
		id:      sid,
		topic:   topic,
		handler: handler,
		options: *opts,
		unsub: func() error {
			b.Lock()
			defer b.Unlock()

			t := b.openTopic(topic)
			t.unsubscribe(sid)

			if t.size() == 0 {
				delete(b.topics, topic)
			}

			return nil
		},
	}

	b.openTopic(topic).subscribe(sub)

	return sub, nil
}

func (b *broker) Shutdown(_ context.Context) error {
	return nil
}

func (b *broker) openTopic(name pubsub.Topic) *topic {
	t, ok := b.topics[name]
	if !ok {
		t = newTopic(name)
		b.topics[name] = t
	}

	return t
}
