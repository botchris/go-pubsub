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

func TestSingleBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a sns topic and a sns broker linked to its queue", func(t *testing.T) {
		cfg := awsConfig(t)
		snsCli := awssns.NewFromConfig(cfg)
		sqsCli := awssqs.NewFromConfig(cfg)

		broker, err := prepareBroker(ctx, sqsCli, snsCli, "test-queue")
		require.NoError(t, err)
		require.NotNil(t, broker)

		defer func() {
			_ = broker.Shutdown(ctx)
		}()

		tRes, err := snsCli.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("test-topic")})
		require.NoError(t, err)
		topicARN := *tRes.TopicArn
		topic := pubsub.Topic(topicARN)

		t.Run("WHEN asking for registered topics THEN one topic is informed", func(t *testing.T) {
			topics, tErr := broker.Topics(ctx)

			require.NoError(t, tErr)
			require.Len(t, topics, 1)
			require.EqualValues(t, topicARN, topics[0].String())
		})

		consumer1 := &consumer{}
		consumer2 := &consumer{}

		t.Run("WHEN registering two subscribers to such topic", func(t *testing.T) {
			sub1 := pubsub.NewSubscriber(consumer1.handle)
			require.NoError(t, broker.Subscribe(ctx, topic, sub1))

			sub2 := pubsub.NewSubscriber(consumer2.handle)
			require.NoError(t, broker.Subscribe(ctx, topic, sub2))

			// wait async subscription to take place
			time.Sleep(time.Second)

			t.Run("THEN sns registers only one subscription as they are the same topic", func(t *testing.T) {
				subs, lErr := snsCli.ListSubscriptions(ctx, &awssns.ListSubscriptionsInput{})

				require.NoError(t, lErr)
				require.Len(t, subs.Subscriptions, 1)
				require.EqualValues(t, topicARN, *subs.Subscriptions[0].TopicArn)
			})
		})

		t.Run("WHEN publishing N messages to the topic", func(t *testing.T) {
			sent := []string{
				"hello world",
				"lorem ipsum",
				"dolorem sit amet",
			}

			for _, msg := range sent {
				require.NoError(t, broker.Publish(ctx, topic, msg))
			}

			t.Run("THEN subscribers of such topic eventually receives the messages only once", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return consumer1.received().hasExactlyOnce(sent...)
				}, 10*time.Second, time.Millisecond*100)

				require.Eventually(t, func() bool {
					return consumer2.received().hasExactlyOnce(sent...)
				}, 10*time.Second, time.Millisecond*100)
			})
		})
	})
}

func TestMultiInstanceBroker(t *testing.T) {
	// This test simulates multiple instances of the same applications share messages in a round-robin fashion.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a sns topic and two brokers attached to the same sqs queue with one subscriber each", func(t *testing.T) {
		cfg := awsConfig(t)
		snsCli := awssns.NewFromConfig(cfg)
		sqsCli := awssqs.NewFromConfig(cfg)

		broker1, err := prepareBroker(ctx, sqsCli, snsCli, "test-queue-b1")
		require.NoError(t, err)
		require.NotNil(t, broker1)

		defer func() {
			_ = broker1.Shutdown(ctx)
		}()

		broker2, err := prepareBroker(ctx, sqsCli, snsCli, "test-queue-b1")
		require.NoError(t, err)
		require.NotNil(t, broker2)

		defer func() {
			_ = broker2.Shutdown(ctx)
		}()

		tRes, err := snsCli.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("test-topic")})
		require.NoError(t, err)
		topicARN := *tRes.TopicArn
		topic := pubsub.Topic(topicARN)

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker1.Subscribe(ctx, topic, sub1))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker2.Subscribe(ctx, topic, sub2))

		// wait async subscription to take place
		time.Sleep(time.Second)

		t.Run("WHEN publishing N messages to the topic using B1", func(t *testing.T) {
			sent := []string{
				"test-message-1",
				"test-message-2",
				"test-message-3",
				"test-message-4",
				"test-message-5",
			}

			for _, msg := range sent {
				require.NoError(t, broker1.Publish(ctx, topic, msg))
			}

			t.Run("THEN eventually subscribers receives all the messages", func(t *testing.T) {
				require.Eventually(t, func() bool {
					r1 := consumer1.received()
					r2 := consumer2.received()

					return r1.merge(r2).hasExactlyOnce(sent...)
				}, 10*time.Second, time.Millisecond*100)
			})
		})
	})
}

func TestMultiHostBroker(t *testing.T) {
	// This test simulates multiple applications reading from the same topic.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a sns topic and two brokers (B1 and B2) with one subscriber each", func(t *testing.T) {
		cfg := awsConfig(t)
		snsCli := awssns.NewFromConfig(cfg)
		sqsCli := awssqs.NewFromConfig(cfg)

		broker1, err := prepareBroker(ctx, sqsCli, snsCli, "test-queue-b1")
		require.NoError(t, err)
		require.NotNil(t, broker1)

		defer func() {
			_ = broker1.Shutdown(ctx)
		}()

		broker2, err := prepareBroker(ctx, sqsCli, snsCli, "test-queue-b2")
		require.NoError(t, err)
		require.NotNil(t, broker2)

		defer func() {
			_ = broker2.Shutdown(ctx)
		}()

		tRes, err := snsCli.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("test-topic")})
		require.NoError(t, err)
		topicARN := *tRes.TopicArn
		topic := pubsub.Topic(topicARN)

		consumer1 := &consumer{}
		sub1 := pubsub.NewSubscriber(consumer1.handle)
		require.NoError(t, broker1.Subscribe(ctx, topic, sub1))

		consumer2 := &consumer{}
		sub2 := pubsub.NewSubscriber(consumer2.handle)
		require.NoError(t, broker2.Subscribe(ctx, topic, sub2))

		// wait async subscription to take place
		time.Sleep(time.Second)

		t.Run("WHEN publishing N messages to the topic using B1", func(t *testing.T) {
			sent := []string{
				"hello world",
				"lorem ipsum",
				"dolorem sit amet",
			}

			for _, msg := range sent {
				require.NoError(t, broker1.Publish(ctx, topic, msg))
			}

			t.Run("THEN subscribers of such topic eventually receives the messages only once", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return consumer1.received().hasExactlyOnce(sent...)
				}, 5*time.Second, time.Millisecond*100)

				require.Eventually(t, func() bool {
					return consumer2.received().hasExactlyOnce(sent...)
				}, 5*time.Second, time.Millisecond*100)
			})
		})
	})
}

type consumer struct {
	rcv queue
	mu  sync.RWMutex
}

type queue []string

func (q queue) merge(q2 queue) queue {
	x := append(q, q2...)

	return x
}

func (q queue) hasExactlyOnce(expected ...string) bool {
	seen := make(map[string]int)

	for _, m1 := range expected {
		for _, m2 := range q {
			if m1 == m2 {
				seen[m1]++

				if seen[m1] > 1 {
					return false
				}
			}
		}
	}

	return len(seen) == len(expected)
}

func (c *consumer) handle(_ context.Context, msg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.rcv = append(c.rcv, msg)

	return nil
}

func (c *consumer) received() queue {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]string, len(c.rcv))
	copy(out, c.rcv)

	return out
}

func prepareBroker(
	ctx context.Context,
	sqsClient *awssqs.Client,
	snsClient *awssns.Client,
	queueName string,
) (pubsub.Broker, error) {
	qRes, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(queueName)})
	if err != nil {
		return nil, err
	}

	queueURL := *qRes.QueueUrl
	encoder := func(msg interface{}) ([]byte, error) { return []byte(msg.(string)), nil }
	decoder := func(data []byte) (interface{}, error) { return string(data), nil }

	return sns.NewBroker(ctx,
		sns.WithSNSClient(snsClient),
		sns.WithSQSClient(sqsClient),
		sns.WithSQSQueueURL(queueURL),
		sns.WithEncoder(encoder),
		sns.WithDecoder(decoder),
		sns.WithWaitTimeSeconds(1),
	)
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
