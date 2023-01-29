package rabbitmq

// Option represents an option that can be passed to redis broker.
type Option func(*options)

type options struct {
	address string
	logger  Logger
}

// WithAddress sets the address of the redis server.
func WithAddress(address string) Option {
	return func(o *options) {
		o.address = address
	}
}

// WithLogger defines a logger for the broker. Default: noop logger.
func WithLogger(logger Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}
