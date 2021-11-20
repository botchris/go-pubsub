package pubsub

import (
	"context"
)

// Topic identifies a particular Topic on which messages can be published.
type Topic string

// String returns the string representation of the Topic.
func (t Topic) String() string {
	return string(t)
}

// Broker defines the interface for a pub/sub broker.
type Broker interface {
	// Publish the given message on the given topic.
	Publish(ctx context.Context, topic Topic, m interface{}) error

	// Subscribe attaches the given Subscriber to the given topic.
	// This method should noop in case of duplicated subscriptions.
	Subscribe(ctx context.Context, topic Topic, subscriber Subscriber) error

	// Unsubscribe removes the given Subscriber from the specified topic.
	// This method should noop if the Subscriber is not attached to the topic.
	Unsubscribe(ctx context.Context, topic Topic, subscriber Subscriber) error

	// Subscriptions retrieves a list of subscribers currently attached to this broker.
	Subscriptions(ctx context.Context) (map[Topic][]Subscriber, error)

	// Shutdown shutdowns all subscribers gracefully.
	Shutdown(ctx context.Context) error
}
