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

	// Subscribe attaches the given handler to the given topic.
	// The same handler could be attached multiple times to the same topic.
	Subscribe(ctx context.Context, topic Topic, handler Handler, option ...SubscribeOption) (Subscription, error)

	// Shutdown gracefully shutdowns all subscriptions.
	Shutdown(ctx context.Context) error
}
