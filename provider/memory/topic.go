package memory

import (
	"context"
	"sync"

	"github.com/botchris/go-pubsub"
)

// topic defines a in-memory topic to which subscriber may subscribe to
type topic struct {
	id          pubsub.Topic
	subscribers map[string]pubsub.Subscription
	sync.RWMutex
}

type publishResult struct {
	subscriber pubsub.Subscription
	err        error
}

// newTopic creates a new topic
func newTopic(id pubsub.Topic) *topic {
	return &topic{
		id:          id,
		subscribers: map[string]pubsub.Subscription{},
	}
}

// publish sends the given message to each subscriber of this topic
func (t *topic) publish(ctx context.Context, m interface{}) []*publishResult {
	t.RLock()
	defer t.RUnlock()

	var errs sync.Map

	for _, s := range t.subscribers {
		subscriber := s
		result := &publishResult{
			subscriber: subscriber,
			err:        nil,
		}

		if err := subscriber.Handler().Deliver(ctx, t.id, m); err != nil {
			result.err = err
		}

		errs.Store(result, struct{}{})
	}

	out := make([]*publishResult, 0)
	errs.Range(func(k, v interface{}) bool {
		out = append(out, k.(*publishResult))

		return true
	})

	return out
}

// subscribe attaches to this topic the given subscriber, attaching multiple times the same subscriber has no effects.
func (t *topic) subscribe(s pubsub.Subscription) {
	t.Lock()
	defer t.Unlock()

	t.subscribers[s.ID()] = s
}

// unsubscribe detaches from this topic the given subscriber, will nop if subscriber is not present.
func (t *topic) unsubscribe(id string) {
	t.Lock()
	defer t.Unlock()

	delete(t.subscribers, id)
}

// size how many subscribers are currently attached to this topic
func (t *topic) size() int {
	t.RLock()
	defer t.RUnlock()

	return len(t.subscribers)
}
