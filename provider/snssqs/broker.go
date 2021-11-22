// Package snssqs provides a simple AWS SNS+SQS broker implementation.
package snssqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/botchris/go-pubsub"
	"github.com/hashicorp/go-multierror"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
)

type broker struct {
	runnerCancel context.CancelFunc
	runnerCtx    context.Context
	options      *options
	topics       *topicsCache
	subs         map[pubsub.Topic]map[string]Subscription
	mu           sync.RWMutex
}

// NewBroker returns a broker that allows to push to AWS SNS topics and
// subscribe messages brokered by SQS. This provider does not (yet) support
// automatic creation of SQS queues or SNS topics, so those will have to exist
// in your infrastructure before attempting to send/receive.
//
// This broker will start running a background goroutine that will poll the SQS
// queue for new messages. Topics must be firstly created on AWS SNS before
// starting this broker, and each topic must be tagged with the "topic-name"
// key on AWS. This is used to hold the name of the topic as seen by the broker
// implementation (`pubsub.topic`). This allows to keep the topic name
// agnostic to their underlying implementation.
//
// IMPORTANT: this broker must be used in conjunction with a Codec middleware in
// order to ensure that the messages are properly encoded and decoded.
// Otherwise, only binary messages will be accepted when publishing or
// delivering messages.
func NewBroker(ctx context.Context, option ...Option) (pubsub.Broker, error) {
	opts := &options{
		deliverTimeout:       5 * time.Second,
		topicsReloadInterval: 60 * time.Second,
		maxMessages:          5,
		visibilityTimeout:    30,
		waitTimeSeconds:      15,
	}

	for _, o := range option {
		o.apply(opts)
	}

	if opts.snsClient == nil {
		return nil, errors.New("no SNS client was provided")
	}

	if opts.sqsClient == nil {
		return nil, errors.New("no SQS client was provided")
	}

	if opts.sqsQueueURL == "" {
		return nil, errors.New("no SQS queue URL was provided")
	}

	ctx, cancel := context.WithCancel(ctx)
	b := &broker{
		runnerCancel: cancel,
		runnerCtx:    ctx,
		options:      opts,
		topics:       newTopicsCache(opts.snsClient),
		subs:         make(map[pubsub.Topic]map[string]Subscription),
	}

	if err := b.topics.reloadCache(ctx); err != nil {
		return nil, err
	}

	defer func() {
		go b.run()
	}()

	return b, nil
}

func (b *broker) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	bytes, isBinary := m.([]byte)
	if !isBinary {
		return fmt.Errorf("expecting message to be of type []byte, but got `%T`", m)
	}

	topicARN, err := b.topics.arnOf(topic)
	if err != nil {
		return err
	}

	_, err = b.options.snsClient.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(string(bytes)),
		TopicArn: aws.String(topicARN),
	})

	if err != nil {
		return err
	}

	return nil
}

func (b *broker) Subscribe(ctx context.Context, topic pubsub.Topic, handler pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	topicARN, err := b.topics.arnOf(topic)
	if err != nil {
		return nil, err
	}

	subSNS, err := b.options.snsClient.Subscribe(ctx, &sns.SubscribeInput{
		Endpoint: aws.String(b.options.sqsQueueURL),
		Protocol: aws.String("sqs"),
		TopicArn: aws.String(topicARN),
	})

	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subs[topic]; !ok {
		b.subs[topic] = make(map[string]Subscription)
	}

	opts := pubsub.NewSubscribeOptions(option...)
	sid := uuid.New()
	unsub := func() error {
		b.mu.Lock()
		defer b.mu.Unlock()

		subs, ok := b.subs[topic]
		if !ok {
			return nil
		}

		sub, ok := subs[sid]
		if !ok {
			return nil
		}

		_, uErr := b.options.snsClient.Unsubscribe(ctx, &sns.UnsubscribeInput{
			SubscriptionArn: aws.String(sub.ARN()),
		})

		if uErr != nil {
			return err
		}

		delete(b.subs[topic], sid)

		return nil
	}

	sub := &awsSubscription{
		arn:          *subSNS.SubscriptionArn,
		Subscription: pubsub.NewSubscription(sid, topic, handler, unsub, opts),
	}

	b.subs[topic][sid] = sub

	return sub, nil
}

func (b *broker) Shutdown(ctx context.Context) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	mErr := &multierror.Error{}

	for _, subs := range b.subs {
		for _, sub := range subs {
			_, err := b.options.snsClient.Unsubscribe(ctx, &sns.UnsubscribeInput{
				SubscriptionArn: aws.String(sub.ARN()),
			})

			if err != nil {
				mErr = multierror.Append(mErr, err)
			}
		}
	}

	b.runnerCancel()

	return mErr
}

func (b *broker) run() {
	done := b.runnerCtx.Done()
	topicTicker := time.NewTicker(b.options.topicsReloadInterval)
	defer topicTicker.Stop()

	for {
		select {
		case <-topicTicker.C:
			_ = b.topics.reloadCache(b.runnerCtx)
			continue
		case <-done:
			return
		default:
		}

		res, err := b.options.sqsClient.ReceiveMessage(b.runnerCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(b.options.sqsQueueURL),
			MaxNumberOfMessages: b.options.maxMessages,
			VisibilityTimeout:   b.options.visibilityTimeout,
			WaitTimeSeconds:     b.options.waitTimeSeconds,
		})

		if err != nil {
			continue
		}

		deliver := make(map[sqsNotification][]Subscription)
		ignore := make([]string, 0)

		for _, n := range res.Messages {
			noty := sqsNotification{}
			if uErr := json.Unmarshal([]byte(*n.Body), &noty); uErr != nil {
				continue
			}

			noty.ReceiptHandle = *n.ReceiptHandle
			topic := b.topics.nameOf(noty.TopicARN)

			b.mu.RLock()
			subs, ok := b.subs[topic]
			b.mu.RUnlock()

			if !ok {
				ignore = append(ignore, noty.ReceiptHandle)
				continue
			}

			deliver[noty] = make([]Subscription, 0)
			for i := range subs {
				deliver[noty] = append(deliver[noty], subs[i])
			}
		}

		for i := range ignore {
			ctx, cancel := context.WithTimeout(b.runnerCtx, b.options.deliverTimeout)
			_, _ = b.changeMsgVisibilityTimeout(ctx, ignore[i], 0)
			cancel()
		}

		var wg sync.WaitGroup

		for noty, subs := range deliver {
			for i := range subs {
				wg.Add(1)

				go func(sub Subscription, msg sqsNotification) {
					defer wg.Done()

					if hErr := b.handleNotification(sub, msg); hErr != nil {
						// TODO: monitor error
						return
					}
				}(subs[i], noty)
			}
		}

		wg.Wait()
	}
}

func (b *broker) handleNotification(sub Subscription, noty sqsNotification) error {
	ctx, cancel := context.WithTimeout(b.runnerCtx, b.options.deliverTimeout)
	defer cancel()

	go b.heartbeatMessage(ctx, noty.ReceiptHandle)

	message := []byte(noty.Message)

	if dErr := sub.Handler().Deliver(ctx, sub.Topic(), message); dErr != nil {
		mErr := &multierror.Error{}
		mErr = multierror.Append(mErr, dErr)

		// return the message back (immediately) to the queue by resetting the visibility timeout
		if _, cErr := b.changeMsgVisibilityTimeout(ctx, noty.ReceiptHandle, 0); cErr != nil {
			mErr = multierror.Append(mErr, cErr)
		}

		return mErr
	}

	if sub.Options().AutoAck {
		input := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(b.options.sqsQueueURL),
			ReceiptHandle: aws.String(noty.ReceiptHandle),
		}

		if _, err := b.options.sqsClient.DeleteMessage(b.runnerCtx, input); err != nil {
			// There was an error removing the message from the queue, so probably the message
			// is still in the queue and will receive it again (although we will never know),
			// so be prepared to process the message again without side effects.
			return nil
		}
	}

	return nil
}

// heartbeatMessage updates message's visibility timeout periodically, so it's not queued again while it keeps being
// processed.
//
// Useful in cases where handler takes more than queue's `visibilityTimeout` to complete.
// IMPORTANT: This function will run until given context is cancelled or when it fails to change visibility.
// see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html#configuring-visibility-timeout
func (b *broker) heartbeatMessage(ctx context.Context, receiptHandle string) {
	timeout := b.options.visibilityTimeout
	ticker := time.NewTicker(time.Second * time.Duration(timeout) / 2)

	defer ticker.Stop()
	done := ctx.Done()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if _, err := b.changeMsgVisibilityTimeout(ctx, receiptHandle, b.options.visibilityTimeout); err != nil {
				return
			}
		}
	}
}

func (b *broker) changeMsgVisibilityTimeout(ctx context.Context, receiptHandle string, visibilityTimeout int32) (*sqs.ChangeMessageVisibilityOutput, error) {
	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(b.options.sqsQueueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: visibilityTimeout,
	}

	return b.options.sqsClient.ChangeMessageVisibility(ctx, input)
}
