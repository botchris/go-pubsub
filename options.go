package pubsub

// SubscribeOptions describes the options for a subscription action.
type SubscribeOptions struct {
	// Group subscriptions with the same group name will create a shared
	// subscription where each one receives a subset of the published messages.
	Group string

	// AutoAck defaults to true. When a handler returns with a nil error the
	// message is acknowledged.
	AutoAck bool

	// Metadata other options for concrete implementations of the Broker
	// interface can be stored as metadata.
	Metadata Metadata
}

// SubscribeOption is a function that configures a SubscribeOptions instance.
type SubscribeOption func(*SubscribeOptions)

// NewSubscribeOptions convenience function for building a SubscribeOptions
// based on given list of options.
func NewSubscribeOptions(opts ...SubscribeOption) SubscribeOptions {
	s := &SubscribeOptions{
		AutoAck:  true,
		Metadata: NewMetadata(),
	}

	for _, o := range opts {
		o(s)
	}

	return *s
}

// DisableAutoAck will disable auto ACKing of messages after they have been
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
func SubscribeMetadata(meta Metadata) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Metadata = meta
	}
}
