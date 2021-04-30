// Package memory provides a simple in-memory Broker.
//
// This broker should never be used for IPC purposes (Inter-Process Communication) as it only works by moving
// messages using local memory.
package memory

import (
	"context"
	"sync"

	"github.com/ChristopherCastro/go-pubsub"
)

// SubscriberErrorHandler used to handle subscribers errors when processing a message.
type SubscriberErrorHandler func(ctx context.Context, topic pubsub.Topic, s *pubsub.Subscriber, m interface{}, err error)

// NopSubscriberErrorHandler an empty error handler
var NopSubscriberErrorHandler = func(ctx context.Context, topic pubsub.Topic, s *pubsub.Subscriber, m interface{}, err error) {}

type broker struct {
	topics        map[pubsub.Topic]*topic
	subErrHandler SubscriberErrorHandler
	sync.RWMutex
}

// NewBroker returns a new in-memory broker instance.
func NewBroker(subErrHandler SubscriberErrorHandler) pubsub.Broker {
	return &broker{
		topics:        make(map[pubsub.Topic]*topic),
		subErrHandler: subErrHandler,
	}
}

func (b *broker) Publish(ctx context.Context, m interface{}, topic pubsub.Topic) error {
	b.RLock()
	defer b.RUnlock()

	t := b.openTopic(topic)
	for _, result := range t.publish(ctx, m) {
		if result.err != nil {
			b.subErrHandler(ctx, t.id, result.subscriber, m, result.err)
		}
	}

	return nil
}

func (b *broker) Subscribe(ctx context.Context, s *pubsub.Subscriber, topic pubsub.Topic) error {
	b.Lock()
	defer b.Unlock()

	b.openTopic(topic).subscribe(s)

	return nil
}

func (b *broker) Topics(ctx context.Context) ([]pubsub.Topic, error) {
	b.RLock()
	defer b.RUnlock()

	keys := make([]pubsub.Topic, 0, len(b.topics))
	for k := range b.topics {
		keys = append(keys, k)
	}

	return keys, nil
}

func (b *broker) Shutdown(ctx context.Context) {}

func (b *broker) openTopic(name pubsub.Topic) *topic {
	t, ok := b.topics[name]
	if !ok {
		t = newTopic(name)
		b.topics[name] = t
	}

	return t
}
