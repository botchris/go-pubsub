package rr

import (
	"sync"
	"sync/atomic"

	"github.com/botchris/go-pubsub"
)

// queue is a queue implementation with round-robin selection algorithm.
type queue struct {
	name      queueName
	items     []pubsub.StoppableSubscription
	locations map[string]int
	next      uint32
	mu        sync.RWMutex
}

type queueName string

func newQueue(name string) *queue {
	return &queue{
		name:      queueName(name),
		items:     make([]pubsub.StoppableSubscription, 0),
		locations: make(map[string]int),
	}
}

func (r *queue) add(subscription pubsub.StoppableSubscription) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := subscription.ID()

	if at, ok := r.locations[id]; ok {
		r.items[at] = subscription

		return
	}

	r.items = append(r.items, subscription)
	r.locations[id] = len(r.items) - 1
}

func (r *queue) remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	at, ok := r.locations[id]
	if !ok {
		return
	}

	r.items = append(r.items[:at], r.items[at+1:]...)
	delete(r.locations, id)
}

func (r *queue) all() []pubsub.StoppableSubscription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]pubsub.StoppableSubscription, len(r.items))
	copy(out, r.items)

	return out
}

func (r *queue) empty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.items) == 0
}

// pick returns the next subscription. It may return nil if there are no
// subscriptions.
func (r *queue) pick() pubsub.StoppableSubscription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.items) == 0 {
		return nil
	}

	n := atomic.AddUint32(&r.next, 1)
	i := (int(n) - 1) % len(r.items)

	return r.items[i]
}
