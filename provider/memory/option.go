package memory

// Option is a broker option.
type Option func(*broker)

// WithPublishErrors sets the error logger for the broker.
func WithPublishErrors() Option {
	return func(b *broker) {
		b.withPublishErrors = true
	}
}
