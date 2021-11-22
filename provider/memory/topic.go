package memory

import (
	"context"
	"sync"

	"github.com/botchris/go-pubsub"
)

// topic defines an in-memory topic to which handlers may subscribe to
type topic struct {
	id            pubsub.Topic
	subscriptions map[string]pubsub.Subscription
	sync.RWMutex
}

type publishResult struct {
	subscription pubsub.Subscription
	err          error
}

// newTopic creates a new topic
func newTopic(id pubsub.Topic) *topic {
	return &topic{
		id:            id,
		subscriptions: map[string]pubsub.Subscription{},
	}
}

// publish sends the given message to each handler of this topic
func (t *topic) publish(ctx context.Context, m interface{}) []*publishResult {
	t.RLock()
	defer t.RUnlock()

	var errs sync.Map

	for _, s := range t.subscriptions {
		result := &publishResult{
			subscription: s,
			err:          nil,
		}

		if err := s.Handler().Deliver(ctx, t.id, m); err != nil {
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

// subscribe attaches to this topic the given subscription, attaching multiple
// times the same subscription has no effects.
func (t *topic) subscribe(s pubsub.Subscription) {
	t.Lock()
	defer t.Unlock()

	t.subscriptions[s.ID()] = s
}

// unsubscribe detaches from this topic the given subscription id, will nop if
// handler is not present.
func (t *topic) unsubscribe(id string) {
	t.Lock()
	defer t.Unlock()

	delete(t.subscriptions, id)
}

// size how many subscriptions are currently attached to this topic
func (t *topic) size() int {
	t.RLock()
	defer t.RUnlock()

	return len(t.subscriptions)
}
