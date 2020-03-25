package pubsub

import (
	"context"
	"sync"
)

// TopicID topic identifier
type TopicID string

// Topic defines a topic to which subscriber may subscribe to
type Topic struct {
	mu          sync.RWMutex
	id          TopicID
	subscribers []Subscriber
}

// NewTopic creates a new topic
func NewTopic(id TopicID) *Topic {
	return &Topic{
		id:          id,
		subscribers: []Subscriber{},
	}
}

// Publish sends the given message to each subscriber of this topic
func (t *Topic) Publish(ctx context.Context, m interface{}) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var wg sync.WaitGroup

	for _, s := range t.subscribers {
		wg.Add(1)
		fn := s

		go func() {
			defer wg.Done()
			fn(ctx, m)
		}()
	}

	wg.Wait()
}

// Subscribe attaches to this topic the given subscriber
func (t *Topic) Subscribe(ctx context.Context, s Subscriber) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.subscribers = append(t.subscribers, s)
}
