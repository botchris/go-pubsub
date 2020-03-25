package pubsub

import (
	"context"
	"sync"
)

// Broker pub-sub broker definition
type Broker interface {
	// Subscribes to the given topics
	Subscribe(ctx context.Context, s Subscriber, topics ...TopicID)

	// Publish the given message onto the given topics
	Publish(ctx context.Context, m interface{}, topics ...TopicID)

	// Retrieves a list of topics registered in this broker
	Topics() []TopicID
}

type broker struct {
	sync.RWMutex
	topics map[TopicID]*Topic
}

// NewBroker returns a new broker instance
func NewBroker() Broker {
	return &broker{
		topics: make(map[TopicID]*Topic),
	}
}

func (b *broker) Subscribe(ctx context.Context, s Subscriber, topics ...TopicID) {
	b.Lock()
	defer b.Unlock()

	for _, t := range topics {
		b.openTopic(t).Subscribe(ctx, s)
	}
}

func (b *broker) Publish(ctx context.Context, m interface{}, topics ...TopicID) {
	b.RLock()
	defer b.RUnlock()

	var wg sync.WaitGroup

	for _, t := range topics {
		wg.Add(1)
		topic := b.openTopic(t)

		go func() {
			defer wg.Done()
			topic.Publish(ctx, m)
		}()
	}

	wg.Wait()
}

func (b *broker) Topics() []TopicID {
	b.RLock()
	defer b.RUnlock()

	keys := make([]TopicID, 0, len(b.topics))
	for k := range b.topics {
		keys = append(keys, k)
	}

	return keys
}

func (b *broker) openTopic(name TopicID) *Topic {
	t, ok := b.topics[name]
	if !ok {
		t = NewTopic(name)
		b.topics[name] = t
	}

	return t
}
