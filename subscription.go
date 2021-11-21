package pubsub

// Subscription represents a handler subscribed to a topic
type Subscription interface {
	ID() string
	Options() SubscribeOptions
	Topic() Topic
	Unsubscribe() error
	Handler() Handler
}

// UnsubscribeFunc represents a function responsible for unsubscribing.
type UnsubscribeFunc func() error

type subscription struct {
	id      string
	topic   Topic
	options SubscribeOptions
	unsub   func() error
	handler Handler
}

// NewSubscription convenience function for creating a new subscriptions.
func NewSubscription(
	id string,
	topic Topic,
	handler Handler,
	unsub UnsubscribeFunc,
	options SubscribeOptions,
) Subscription {
	if unsub == nil {
		unsub = func() error {
			return nil
		}
	}

	return &subscription{
		id:      id,
		topic:   topic,
		options: options,
		unsub:   unsub,
		handler: handler,
	}
}

func (s *subscription) ID() string {
	return s.id
}

func (s *subscription) Options() SubscribeOptions {
	return s.options
}

func (s *subscription) Topic() Topic {
	return s.topic
}

func (s *subscription) Unsubscribe() error {
	return s.unsub()
}

func (s *subscription) Handler() Handler {
	return s.handler
}
