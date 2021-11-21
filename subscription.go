package pubsub

// Subscription represents a handler subscribed to a topic
type Subscription interface {
	ID() string
	Options() SubscribeOptions
	Topic() Topic
	Unsubscribe() error
	Handler() Handler
}
