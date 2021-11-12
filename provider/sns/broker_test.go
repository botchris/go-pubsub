package sns_test

import (
	"context"
	"sync"
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

	t.Run("GIVEN a sns topic, a sqs queue and a sns broker linked to such queue", func(t *testing.T) {
		cfg := awsConfig(t)
		snsCli := awssns.NewFromConfig(cfg)
		sqsCli := awssqs.NewFromConfig(cfg)
		queueName := "test-queue"
		topicName := "test-topic"

		tRes, err := snsCli.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String(topicName)})
		require.NoError(t, err)
		topicARN := *tRes.TopicArn
		topic := pubsub.Topic(topicARN)

		qRes, err := sqsCli.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(queueName)})
		require.NoError(t, err)
		queueURL := *qRes.QueueUrl

		encoder := func(msg interface{}) ([]byte, error) { return []byte(msg.(string)), nil }
		decoder := func(data []byte) (interface{}, error) { return string(data), nil }
		broker := sns.NewBroker(ctx, snsCli, sqsCli, queueURL, sns.WithEncoder(encoder), sns.WithDecoder(decoder), sns.WithWaitTimeSeconds(1))
		require.NotNil(t, broker)

		t.Run("WHEN asking for registered topics THEN one topic is informed", func(t *testing.T) {
			topics, err := broker.Topics(ctx)
			require.NoError(t, err)
			require.Len(t, topics, 1)
			require.EqualValues(t, topicARN, topics[0].String())
		})

		var receiveMu sync.RWMutex
		received := ""
		receiveCount := 0
		handler := func(ctx context.Context, msg string) error {
			receiveMu.Lock()
			defer receiveMu.Unlock()

			received = msg
			receiveCount++

			return nil
		}

		t.Run("WHEN adding a new subscriber to such topic a message to such topic", func(t *testing.T) {
			sub := pubsub.NewSubscriber(handler)

			require.NoError(t, broker.Subscribe(ctx, topic, sub))

			t.Run("THEN sns registers such subscription", func(t *testing.T) {
				subs, err := snsCli.ListSubscriptions(ctx, &awssns.ListSubscriptionsInput{})
				require.NoError(t, err)
				require.Len(t, subs.Subscriptions, 1)
				require.EqualValues(t, topicARN, *subs.Subscriptions[0].TopicArn)
			})
		})

		t.Run("WHEN publishing a message to the topic", func(t *testing.T) {
			send := "hello world"
			require.NoError(t, broker.Publish(ctx, topic, send))

			t.Run("THEN subscriber of such topic eventually receives the message only once", func(t *testing.T) {
				require.Eventually(t, func() bool {
					receiveMu.RLock()
					defer receiveMu.RUnlock()

					return received == send && receiveCount == 1
				}, 10*time.Second, time.Millisecond*100)
			})
		})
	})
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
