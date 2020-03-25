package pubsub

import "context"

// Subscriber definition of topic subscriber
type Subscriber func(ctx context.Context, m interface{})
