package snssqs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/middleware/codec"
	"github.com/botchris/go-pubsub/provider/snssqs"
	"github.com/stretchr/testify/require"
)

func TestSingleBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN a sns topic and a sns broker linked to its queue", func(t *testing.T) {
		cfg := awsConfig(t)
		snsCli := awssns.NewFromConfig(cfg)
		sqsCli := awssqs.NewFromConfig(cfg)

		topic := pubsub.Topic("test-topic")
		topicARN, err := prepareTopic(ctx, snsCli, "test-topic")
		require.NoError(t, err)
		require.NotEmpty(t, topicARN)

		broker, err := prepareBroker(ctx, sqsCli, snsCli, "test-queue")
		require.NoError(t, err)
		require.NotNil(t, broker)

		defer func() {
			_ = broker.Shutdown(ctx)
		}()

		consumer1 := &consumer{}
		consumer2 := &consumer{}

		t.Run("WHEN registering two subscriptions to such topic", func(t *testing.T) {
			h1 := pubsub.NewHandler(consumer1.handle)
			_, err = broker.Subscribe(ctx, topic, h1)
			require.NoError(t, err)

			h2 := pubsub.NewHandler(consumer2.handle)
			_, err = broker.Subscribe(ctx, topic, h2)
			require.NoError(t, err)

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

			t.Run("THEN subscriptions of such topic eventually receives the messages only once", func(t *testing.T) {
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

	t.Run("GIVEN a sns topic and two brokers attached to the same sqs queue with one subscriptio each", func(t *testing.T) {
		cfg := awsConfig(t)
		snsCli := awssns.NewFromConfig(cfg)
		sqsCli := awssqs.NewFromConfig(cfg)

		topic := pubsub.Topic("test-topic")
		topicARN, err := prepareTopic(ctx, snsCli, "test-topic")
		require.NoError(t, err)
		require.NotEmpty(t, topicARN)

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

		consumer1 := &consumer{}
		h1 := pubsub.NewHandler(consumer1.handle)
		_, err = broker1.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		consumer2 := &consumer{}
		h2 := pubsub.NewHandler(consumer2.handle)
		_, err = broker2.Subscribe(ctx, topic, h2)
		require.NoError(t, err)

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

			t.Run("THEN eventually subscriptions receives all the messages", func(t *testing.T) {
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

	t.Run("GIVEN a sns topic and two brokers (B1 and B2) with one subscription each", func(t *testing.T) {
		cfg := awsConfig(t)
		snsCli := awssns.NewFromConfig(cfg)
		sqsCli := awssqs.NewFromConfig(cfg)

		topic := pubsub.Topic("test-topic")
		topicARN, err := prepareTopic(ctx, snsCli, "test-topic")
		require.NoError(t, err)
		require.NotEmpty(t, topicARN)

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

		consumer1 := &consumer{}
		h1 := pubsub.NewHandler(consumer1.handle)
		_, err = broker1.Subscribe(ctx, topic, h1)
		require.NoError(t, err)

		consumer2 := &consumer{}
		h2 := pubsub.NewHandler(consumer2.handle)
		_, err = broker2.Subscribe(ctx, topic, h2)
		require.NoError(t, err)

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

			t.Run("THEN subscriptions of such topic eventually receives the messages only once", func(t *testing.T) {
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

func (c *consumer) handle(_ context.Context, _ pubsub.Topic, msg string) error {
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
	broker, err := snssqs.NewBroker(ctx,
		snssqs.WithSNSClient(snsClient),
		snssqs.WithSQSClient(sqsClient),
		snssqs.WithSQSQueueURL(queueURL),
		snssqs.WithWaitTimeSeconds(1),
	)

	if err != nil {
		return nil, err
	}

	return codec.NewCodecMiddleware(broker, codec.JSON), nil
}

func prepareTopic(ctx context.Context, cli *awssns.Client, topic pubsub.Topic) (string, error) {
	if err := flushTopics(ctx, cli); err != nil {
		return "", err
	}

	tRes, err := cli.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String(topic.String()),
		Tags: []types.Tag{
			{
				Key:   aws.String("topic-name"),
				Value: aws.String(topic.String()),
			},
		},
	})

	if err != nil {
		return "", err
	}

	return *tRes.TopicArn, nil
}

func flushTopics(ctx context.Context, cli *awssns.Client) error {
	var next string

	res, err := cli.ListTopics(ctx, &awssns.ListTopicsInput{})
	if err != nil {
		return err
	}

	if res.NextToken != nil {
		next = *res.NextToken
	}

	for i := range res.Topics {
		_, _ = cli.DeleteTopic(ctx, &awssns.DeleteTopicInput{
			TopicArn: res.Topics[i].TopicArn,
		})
	}

	for next != "" {
		res, err = cli.ListTopics(ctx, &awssns.ListTopicsInput{
			NextToken: aws.String(next),
		})

		if err != nil {
			return err
		}

		if res.NextToken != nil {
			next = *res.NextToken
		}

		for i := range res.Topics {
			_, _ = cli.DeleteTopic(ctx, &awssns.DeleteTopicInput{
				TopicArn: res.Topics[i].TopicArn,
			})
		}
	}

	return nil
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
