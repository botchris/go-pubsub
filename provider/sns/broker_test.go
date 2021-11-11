package sns_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/sns"
	"github.com/stretchr/testify/require"
)

func TestBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := awsConfig(t)
	snsCli := awssns.NewFromConfig(cfg)
	sqsCli := awssqs.NewFromConfig(cfg)
	queueName := "test-queue"
	topicName := "test-topic"

	tRes, err := snsCli.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := *tRes.TopicArn
	topic := pubsub.Topic(topicARN)

	qRes, err := sqsCli.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	queueURL := *qRes.QueueUrl

	encoder := func(msg interface{}) ([]byte, error) {
		return []byte(msg.(string)), nil
	}

	decoder := func(data []byte) (interface{}, error) {
		return string(data), nil
	}

	broker := sns.NewBroker(ctx, snsCli, sqsCli, queueURL, sns.WithEncoder(encoder), sns.WithDecoder(decoder), sns.WithWaitTimeSeconds(1))
	require.NotNil(t, broker)

	topics, err := broker.Topics(ctx)
	require.NoError(t, err)
	require.Len(t, topics, 1)

	received := ""
	sub := pubsub.NewSubscriber(func(ctx context.Context, msg string) error {
		received = msg

		return nil
	})

	require.NoError(t, broker.Subscribe(ctx, topic, sub))
	subs, err := snsCli.ListSubscriptions(ctx, &awssns.ListSubscriptionsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, subs.Subscriptions)

	send := "hello world"
	require.NoError(t, broker.Publish(ctx, topic, send))

	require.Eventually(t, func() bool {
		return received == send
	}, 10*time.Second, time.Millisecond*100)
}

func awsConfig(t *testing.T) aws.Config {
	customResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           "http://localhost:4566",
			SigningRegion: "us-east-1",
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolver(customResolver),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				SessionToken:    "test",
			},
		}),
	)

	require.NoError(t, err)

	return awsCfg
}
