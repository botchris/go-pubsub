package pubsub

// SubscribeOptions describes the options for a subscription.
type SubscribeOptions struct {
	// AutoAck defaults to true. When a handler returns with a nil error the
	/// message is acked.
	AutoAck bool

	// Queue subscribers with the same queue name will create a shared subscription
	// where each receives a subset of messages.
	Queue string

	// Metadata other options for implementations of the Broker interface
	// can be stored as metadata.
	Metadata map[string]interface{}
}

// NewSubscribeOptions builds a new SubscribeOptions.
func NewSubscribeOptions() *SubscribeOptions {
	return &SubscribeOptions{
		AutoAck:  true,
		Metadata: make(map[string]interface{}),
	}
}

// SubscribeOption is a function that configures SubscribeOptions.
type SubscribeOption func(*SubscribeOptions)

// WithQueue sets the name of the queue to share messages on.
func WithQueue(name string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Queue = name
	}
}

// DisableAutoAck will disable auto acking of messages after they have been
// handled.
func DisableAutoAck() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.AutoAck = false
	}
}

// SubscribeMetadata set subscription metadata.
func SubscribeMetadata(meta map[string]interface{}) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Metadata = meta
	}
}
