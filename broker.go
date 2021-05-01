package pubsub

import (
	"context"
)

// Topic represents/identifies a particular Topic
type Topic string

// Broker pub-sub broker definition
type Broker interface {
	// Publish the given message onto the given topic
	Publish(ctx context.Context, topic Topic, m interface{}) error

	// Subscribe subscribes to the given topic
	Subscribe(ctx context.Context, topic Topic, subscriber *Subscriber) error

	// Unsubscribe removes the given subscriber from the specified topic
	Unsubscribe(ctx context.Context, topic Topic, subscriber *Subscriber) error

	// Topics retrieves a list of topics registered in this broker
	Topics(ctx context.Context) ([]Topic, error)

	// Shutdown shutdowns all subscribers gracefully
	Shutdown(ctx context.Context) error
}
