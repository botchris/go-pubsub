package sns

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/botchris/go-pubsub"
)

// AWSSNSAPI captures the subset of the AWS SNS API that we need.
type AWSSNSAPI interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
	Subscribe(ctx context.Context, params *sns.SubscribeInput, optFns ...func(*sns.Options)) (*sns.SubscribeOutput, error)
	Unsubscribe(ctx context.Context, params *sns.UnsubscribeInput, optFns ...func(*sns.Options)) (*sns.UnsubscribeOutput, error)
	ListTopics(ctx context.Context, params *sns.ListTopicsInput, optFns ...func(*sns.Options)) (*sns.ListTopicsOutput, error)
}

// AWSSQSAPI captures the subset of the AWS SQS API that we need.
type AWSSQSAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// sqsNotification represents a SQS message that we received from SNS.
type sqsNotification struct {
	Type             string       `json:"Type,omitempty"`
	ReceiptHandle    string       `json:"ReceiptHandle,omitempty"`
	MessageID        string       `json:"MessageId,omitempty"`
	TopicARN         pubsub.Topic `json:"TopicArn,omitempty"`
	Message          string       `json:"Message,omitempty"`
	Timestamp        time.Time    `json:"Timestamp,omitempty"`
	SignatureVersion string       `json:"SignatureVersion,omitempty"`
	Signature        string       `json:"Signature,omitempty"`
	SigningCertURL   string       `json:"SigningCertURL,omitempty"`
}
