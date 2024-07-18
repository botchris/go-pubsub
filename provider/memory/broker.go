// Package memory provides a simple in-memory Broker, which moves messages
// using local memory. Suitable for testing or as a simple "Event Bus" or
// "Event Dispatcher" replacement.
//
// This broker should never be used for IPC purposes
// (Inter-Process Communication) as it only works by moving messages using
// local memory.
package memory

import (
	"context"
	"sync"

	"github.com/botchris/go-pubsub"
	"github.com/hashicorp/go-multierror"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
)

type broker struct {
	topics            map[pubsub.Topic]*topic
	withPublishErrors bool
	sync.RWMutex
}

// NewBroker returns a new in-memory broker instance.
func NewBroker(ops ...Option) pubsub.Broker {
	b := &broker{
		topics: make(map[pubsub.Topic]*topic),
	}

	for _, op := range ops {
		op(b)
	}

	return b
}

func (b *broker) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	b.Lock()
	t := b.openTopic(topic)
	b.Unlock()

	errors := &multierror.Error{}

	for _, result := range t.publish(ctx, m) {
		if result.err != nil && !b.withPublishErrors {
			continue
		}

		if result.err != nil && b.withPublishErrors {
			errors = multierror.Append(errors, result.err)
		}
	}

	return errors.ErrorOrNil()
}

func (b *broker) Subscribe(_ context.Context, topic pubsub.Topic, handler pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	b.Lock()
	defer b.Unlock()

	opts := pubsub.NewSubscribeOptions(option...)
	sid := uuid.New()
	unsub := func() error {
		b.Lock()
		defer b.Unlock()

		t := b.openTopic(topic)
		t.unsubscribe(sid)

		if t.size() == 0 {
			delete(b.topics, topic)
		}

		return nil
	}

	sub := pubsub.NewSubscription(sid, topic, handler, unsub, opts)

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
