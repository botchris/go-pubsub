package util

import (
	"sync"

	"github.com/botchris/go-pubsub"
)

// SubscribersCollection represents a collection of subscriptions organized by
// topic and queues.
type SubscribersCollection struct {
	mu       sync.RWMutex
	wildcard map[pubsub.Topic]*queue
	subs     map[pubsub.Topic]map[queueName]*queue
}

// NewSubscribersCollection builds a new SubscribersCollection collection.
func NewSubscribersCollection() *SubscribersCollection {
	return &SubscribersCollection{
		wildcard: make(map[pubsub.Topic]*queue),
		subs:     make(map[pubsub.Topic]map[queueName]*queue),
	}
}

// Add registers a new subscription.
func (s *SubscribersCollection) Add(subscription *Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if subscription.Options.Queue == "" {
		if _, ok := s.wildcard[subscription.Topic]; !ok {
			s.wildcard[subscription.Topic] = newQueue("*")
		}

		s.wildcard[subscription.Topic].add(subscription)

		return
	}

	if s.subs == nil {
		s.subs = make(map[pubsub.Topic]map[queueName]*queue)
	}

	if _, ok := s.subs[subscription.Topic]; !ok {
		s.subs[subscription.Topic] = make(map[queueName]*queue)
	}

	if _, ok := s.subs[subscription.Topic][queueName(subscription.Options.Queue)]; !ok {
		s.subs[subscription.Topic][queueName(subscription.Options.Queue)] = newQueue(subscription.Options.Queue)
	}

	s.subs[subscription.Topic][queueName(subscription.Options.Queue)].add(subscription)
}

// RemoveFromTopic unregisters the specified subscriber id from the specified
// topic.
//
// Note that the same subscriber id can be registered to multiple topics, and
// under multiple queues for the same topic. This method will unregister from
// every queue for the specified topic.
func (s *SubscribersCollection) RemoveFromTopic(topic pubsub.Topic, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.subs[topic]; ok {
		if _, ok := s.subs[topic]; ok {
			for qName, q := range s.subs[topic] {
				q.remove(id)

				if q.empty() {
					delete(s.subs[topic], qName)
				}

				if len(s.subs[topic]) == 0 {
					delete(s.subs, topic)
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
func (s *SubscribersCollection) HasTopic(topic pubsub.Topic) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.subs[topic]; ok {
		return true
	}

	if _, ok := s.wildcard[topic]; ok {
		return true
	}

	return false
}

// Receptors retrieves a list of subscriptions candidates to handle an arriving
// message to the specified topic.
func (s *SubscribersCollection) Receptors(topic pubsub.Topic) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var receptors []*Subscription

	if _, ok := s.subs[topic]; ok {
		for _, q := range s.subs[topic] {
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
func (s *SubscribersCollection) GracefulStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, qu := range s.subs {
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

	s.subs = make(map[pubsub.Topic]map[queueName]*queue)
	s.wildcard = make(map[pubsub.Topic]*queue)
}
