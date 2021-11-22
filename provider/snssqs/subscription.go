package snssqs

import "github.com/botchris/go-pubsub"

// Subscription represents a subscription to a SNS topic.
type Subscription interface {
	pubsub.Subscription
	ARN() string
}

type subscription struct {
	arn string
	pubsub.Subscription
}

func (s *subscription) ARN() string {
	return s.arn
}
