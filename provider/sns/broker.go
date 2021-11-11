// Package sns provides a simple AWS SNS based broker implementation.
package sns

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/botchris/go-pubsub"
	"github.com/hashicorp/go-multierror"
)

type broker struct {
	snsClient    AWSSNSAPI
	sqsClient    AWSSQSAPI
	sqsQueueURL  string
	runnerCancel context.CancelFunc
	runnerCtx    context.Context
	subs         map[pubsub.Topic]map[string]*subscription
	options      *options
	mu           sync.RWMutex
}

type subscription struct {
	arn     string
	topic   pubsub.Topic
	handler *pubsub.Subscriber
}

// NewBroker returns a broker that uses AWS SNS service for pub/sub messaging over a SQS queue.
func NewBroker(
	ctx context.Context,
	snsClient AWSSNSAPI,
	sqsClient AWSSQSAPI,
	sqsQueueURL string,
	option ...Option,
) pubsub.Broker {
	opts := &options{
		ctx:               ctx,
		deliverTimeout:    3 * time.Second,
		maxMessages:       5,
		visibilityTimeout: 30,
		waitTimeSeconds:   15,
	}

	for _, o := range option {
		o.apply(opts)
	}

	ctx, cancel := context.WithCancel(opts.ctx)

	b := &broker{
		snsClient:    snsClient,
		sqsClient:    sqsClient,
		sqsQueueURL:  sqsQueueURL,
		runnerCancel: cancel,
		runnerCtx:    ctx,
		options:      opts,
		subs:         make(map[pubsub.Topic]map[string]*subscription),
	}

	go b.run()

	return b
}

func (b *broker) Publish(ctx context.Context, topic pubsub.Topic, m interface{}) error {
	if b.options.encoder == nil {
		return ErrNoEncoder
	}

	message, err := b.options.encoder(m)
	if err != nil {
		return err
	}

	_, err = b.snsClient.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(string(message)),
		TopicArn: aws.String(string(topic)),
	})

	if err != nil {
		return err
	}

	return nil
}

func (b *broker) Subscribe(ctx context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	if b.options.decoder == nil {
		return ErrNoEncoder
	}

	sub, err := b.snsClient.Subscribe(ctx, &sns.SubscribeInput{
		Endpoint: aws.String(b.sqsQueueURL),
		Protocol: aws.String("sqs"),
		TopicArn: aws.String(string(topic)),
	})

	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subs[topic]; !ok {
		b.subs[topic] = make(map[string]*subscription)
	}

	b.subs[topic][subscriber.ID()] = &subscription{
		arn:     *sub.SubscriptionArn,
		topic:   topic,
		handler: subscriber,
	}

	return nil
}

func (b *broker) Unsubscribe(ctx context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.subs[topic]
	if !ok {
		return nil
	}

	sub, ok := subs[subscriber.ID()]
	if !ok {
		return nil
	}

	_, err := b.snsClient.Unsubscribe(ctx, &sns.UnsubscribeInput{
		SubscriptionArn: aws.String(sub.arn),
	})

	if err != nil {
		return err
	}

	delete(b.subs[topic], subscriber.ID())

	return nil
}

func (b *broker) Topics(ctx context.Context) ([]pubsub.Topic, error) {
	var next string

	out := make([]pubsub.Topic, 0)
	res, err := b.snsClient.ListTopics(ctx, &sns.ListTopicsInput{})

	if err != nil {
		return out, err
	}

	if res.NextToken != nil {
		next = *res.NextToken
	}

	for i := range res.Topics {
		out = append(out, pubsub.Topic(*res.Topics[i].TopicArn))
	}

	for next != "" {
		res, err = b.snsClient.ListTopics(ctx, &sns.ListTopicsInput{
			NextToken: aws.String(next),
		})

		if err != nil {
			return out, err
		}

		if res.NextToken != nil {
			next = *res.NextToken
		}

		for i := range res.Topics {
			out = append(out, pubsub.Topic(*res.Topics[i].TopicArn))
		}
	}

	return out, nil
}

func (b *broker) Shutdown(ctx context.Context) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	mErr := &multierror.Error{}

	for _, subs := range b.subs {
		for _, sub := range subs {
			_, err := b.snsClient.Unsubscribe(ctx, &sns.UnsubscribeInput{
				SubscriptionArn: aws.String(sub.arn),
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

	for {
		select {
		case <-done:
			return
		default:
		}

		res, err := b.sqsClient.ReceiveMessage(b.runnerCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(b.sqsQueueURL),
			MaxNumberOfMessages: b.options.maxMessages,
			VisibilityTimeout:   b.options.visibilityTimeout,
			WaitTimeSeconds:     b.options.waitTimeSeconds,
		})

		if err != nil {
			continue
		}

		deliver := make(map[sqsNotification][]*subscription)
		ignore := make([]string, 0)

		for _, n := range res.Messages {
			noty := sqsNotification{}
			if uErr := json.Unmarshal([]byte(*n.Body), &noty); uErr != nil {
				continue
			}

			noty.ReceiptHandle = *n.ReceiptHandle

			b.mu.RLock()
			subs, ok := b.subs[noty.TopicARN]
			b.mu.RUnlock()

			if !ok {
				ignore = append(ignore, noty.ReceiptHandle)
				continue
			}

			deliver[noty] = make([]*subscription, 0)
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

				go func(sub *subscription, msg sqsNotification) {
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

func (b *broker) handleNotification(sub *subscription, noty sqsNotification) error {
	ctx, cancel := context.WithTimeout(b.runnerCtx, b.options.deliverTimeout)
	defer cancel()

	go b.heartbeatMessage(ctx, noty.ReceiptHandle)

	body := []byte(noty.Message)
	message, err := b.options.decoder(body)
	if err != nil {
		return err
	}

	if dErr := sub.handler.Deliver(ctx, message); dErr != nil {
		mErr := &multierror.Error{}
		mErr = multierror.Append(mErr, dErr)

		// return the message back (immediately) to the queue by resetting the visibility timeout
		if _, cErr := b.changeMsgVisibilityTimeout(ctx, noty.ReceiptHandle, 0); cErr != nil {
			mErr = multierror.Append(mErr, cErr)
		}

		return mErr
	}

	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(b.sqsQueueURL),
		ReceiptHandle: aws.String(noty.ReceiptHandle),
	}

	if _, err = b.sqsClient.DeleteMessage(b.runnerCtx, input); err != nil {
		// There was an error removing the message from the queue, so probably the message
		// is still in the queue and will receive it again (although we will never know),
		// so be prepared to process the message again without side effects.
		return nil
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
		QueueUrl:          aws.String(b.sqsQueueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: visibilityTimeout,
	}

	return b.sqsClient.ChangeMessageVisibility(ctx, input)
}
