package util

import (
	"sync"

	"github.com/botchris/go-pubsub"
)

// SubscriptionsCollection represents a collection of subscriptions organized by
// topic and queues. This object is thread-safe and can be used by multiple
// goroutines.
type SubscriptionsCollection struct {
	// wildcard holds subscriptions that do not share a topic queue.
	wildcard map[pubsub.Topic]*queue

	// byQueue holds subscriptions that share a topic queue.
	byQueue map[pubsub.Topic]map[queueName]*queue

	// mu protects concurrent access to the collection.
	mu sync.RWMutex
}

// NewSubscriptionsCollection builds a new SubscriptionsCollection collection.
func NewSubscriptionsCollection() *SubscriptionsCollection {
	return &SubscriptionsCollection{
		wildcard: make(map[pubsub.Topic]*queue),
		byQueue:  make(map[pubsub.Topic]map[queueName]*queue),
	}
}

// Add registers a new subscription.
func (s *SubscriptionsCollection) Add(subscription *Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if subscription.Options.Queue == "" {
		if _, ok := s.wildcard[subscription.Topic]; !ok {
			s.wildcard[subscription.Topic] = newQueue("*")
		}

		s.wildcard[subscription.Topic].add(subscription)

		return
	}

	if s.byQueue == nil {
		s.byQueue = make(map[pubsub.Topic]map[queueName]*queue)
	}

	if _, ok := s.byQueue[subscription.Topic]; !ok {
		s.byQueue[subscription.Topic] = make(map[queueName]*queue)
	}

	if _, ok := s.byQueue[subscription.Topic][queueName(subscription.Options.Queue)]; !ok {
		s.byQueue[subscription.Topic][queueName(subscription.Options.Queue)] = newQueue(subscription.Options.Queue)
	}

	s.byQueue[subscription.Topic][queueName(subscription.Options.Queue)].add(subscription)
}

// RemoveFromTopic unregisters the specified subscriber id from the specified
// topic.
//
// Note that the same subscriber id can be registered to multiple topics, and
// under multiple queues for the same topic. This method will unregister from
// every queue for the specified topic.
//
// When the last subscriber is removed from a topic, the topic will be removed
// as well, so calls to `HasTopic` will return false.
func (s *SubscriptionsCollection) RemoveFromTopic(topic pubsub.Topic, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.byQueue[topic]; ok {
		if _, ok := s.byQueue[topic]; ok {
			for qName, q := range s.byQueue[topic] {
				q.remove(id)

				if q.empty() {
					delete(s.byQueue[topic], qName)
				}

				if len(s.byQueue[topic]) == 0 {
					delete(s.byQueue, topic)
				}
			}
		}
	}

	if catchers, ok := s.wildcard[topic]; ok {
		catchers.remove(id)

		if catchers.empty() {
			delete(s.wildcard, topic)
		}
	}
}

// HasTopic whether there is any subscription for the specified topic.
func (s *SubscriptionsCollection) HasTopic(topic pubsub.Topic) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.byQueue[topic]; ok {
		return true
	}

	if _, ok := s.wildcard[topic]; ok {
		return true
	}

	return false
}

// Receptors retrieves a list of subscriptions candidates to handle an arriving
// message to the specified topic.
func (s *SubscriptionsCollection) Receptors(topic pubsub.Topic) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var receptors []*Subscription

	if _, ok := s.byQueue[topic]; ok {
		for _, q := range s.byQueue[topic] {
			if item := q.pick(); item != nil {
				receptors = append(receptors, item)
			}
		}
	}

	if catchAll, ok := s.wildcard[topic]; ok {
		receptors = append(receptors, catchAll.all()...)
	}

	return receptors
}

// GracefulStop purges this collection and signals all subscribers to stop.
func (s *SubscriptionsCollection) GracefulStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, qu := range s.byQueue {
		for _, q := range qu {
			for _, sub := range q.all() {
				sub.Stop()
			}
		}
	}

	for _, q := range s.wildcard {
		for _, sub := range q.all() {
			sub.Stop()
		}
	}

	s.byQueue = make(map[pubsub.Topic]map[queueName]*queue)
	s.wildcard = make(map[pubsub.Topic]*queue)
}
