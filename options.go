package pubsub

// SubscribeOptions describes the options for a subscription action.
type SubscribeOptions struct {
	// Group subscribers with the same group name will create a shared
	// subscription where each one receives a subset of the published messages.
	Group string

	// AutoAck defaults to true. When a handler returns with a nil error the
	/// message is acked.
	AutoAck bool

	// Metadata other options for implementations of the Broker interface
	// can be stored as metadata.
	Metadata map[string]interface{}
}

// DefaultSubscribeOptions builds a new SubscribeOptions.
func DefaultSubscribeOptions() *SubscribeOptions {
	return &SubscribeOptions{
		AutoAck:  true,
		Metadata: make(map[string]interface{}),
	}
}

// SubscribeOption is a function that configures a SubscribeOptions instance.
type SubscribeOption func(*SubscribeOptions)

// DisableAutoAck will disable auto acking of messages after they have been
// handled.
func DisableAutoAck() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.AutoAck = false
	}
}

// WithGroup sets the name of the group to share messages on.
func WithGroup(name string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Group = name
	}
}

// SubscribeMetadata set subscription metadata.
func SubscribeMetadata(meta map[string]interface{}) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Metadata = meta
	}
}
