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

// Publisher is a convenience definition which extract topic-publishing behavior
// into an independent interface.
//
// This is specially useful when needing to publish messages without having to
// expose  the entire Broker implementation.
type Publisher interface {
	// Publish the given message on the given topic.
	Publish(ctx context.Context, topic Topic, m interface{}) error
}

// Subscriber is a convenience definition which extract topic-subscription
// behavior into an independent interface.
//
// This is specially useful when needing to subscribe to topics without having
// to expose the entire Broker implementation.
type Subscriber interface {
	// Subscribe attaches the given handler to the given topic.
	//
	// The same Handler can be reused and attached multiple times to the same
	// or distinct topics.
	Subscribe(ctx context.Context, topic Topic, handler Handler, option ...SubscribeOption) (Subscription, error)
}

// Shutdowner provides a method that can manually trigger the shutdown of the
// Broker by gracefully closing each subscription.
//
// Use this interface when you need to manually shutdown the Broker. Use with
// care, as it will prevent the broker from publishing or receiving any
// messages.
type Shutdowner interface {
	// Shutdown gracefully shutdowns all subscriptions.
	//
	// The provided context acts as a hard-limit cancellation for the shutdown
	// process.
	Shutdown(ctx context.Context) error
}

// Broker defines the interface for a pub/sub broker.
type Broker interface {
	Publisher
	Subscriber
	Shutdowner
}
