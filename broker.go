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

// Broker pub-sub broker definition.
type Broker interface {
	// Publish the given message onto the given topic
	Publish(ctx context.Context, topic Topic, m interface{}) error

	// Subscribe subscribes to the given topic
	Subscribe(ctx context.Context, topic Topic, subscriber *Subscriber) error

	// Unsubscribe removes the given subscriber from the specified topic
	Unsubscribe(ctx context.Context, topic Topic, subscriber *Subscriber) error

	// Subscriptions retrieves a list of subscribers currently attached to this broker.
	Subscriptions(ctx context.Context) (map[Topic][]*Subscriber, error)

	// Shutdown shutdowns all subscribers gracefully
	Shutdown(ctx context.Context) error
}
