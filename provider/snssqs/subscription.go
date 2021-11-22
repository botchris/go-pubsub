package snssqs

import "github.com/botchris/go-pubsub"

// Subscription represents a subscription to SNS topic.
type Subscription interface {
	pubsub.Subscription
	ARN() string
}

type awsSubscription struct {
	arn string
	pubsub.Subscription
}

func (s *awsSubscription) ARN() string {
	return s.arn
}
