package kmq

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/kubemq-io/kubemq-go"
)

type broker struct {
	ctx     context.Context
	client  *kubemq.EventsClient
	options *options
	sender  func(msg *kubemq.Event) error
	subs    map[pubsub.Topic]map[string]*subscription
	mu      sync.RWMutex
}

type subscription struct {
	cancel     context.CancelFunc
	topic      pubsub.Topic
	subscriber pubsub.Subscriber
}

// NewBroker creates a new broker instance that uses KubeMQ over gRPC streams.
// This broker will start consuming right on its creation and as long as the
// given context keeps alive.
//
// IMPORTANT: this broker must be used in conjunction with a Codec middleware in
// order to ensure that the messages are properly encoded and decoded.
// Otherwise, only binary messages will be accepted when publishing or
// delivering messages.
func NewBroker(ctx context.Context, option ...Option) (pubsub.Broker, error) {
	opts := &options{
		serverPort:       50000,
		deliverTimeout:   5 * time.Second,
		onStreamError:    func(err error) {},
		onSubscribeError: func(err error) {},
		autoReconnect:    true,
	}

	for _, o := range option {
		o.apply(opts)
	}

	if opts.serverHost == "" {
		return nil, errors.New("no server host was provided")
	}

	if opts.serverPort <= 0 {
		return nil, errors.New("no server port was provided")
	}

	client, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress(opts.serverHost, opts.serverPort),
		kubemq.WithClientId(opts.clientID),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithCheckConnection(true),
		kubemq.WithAutoReconnect(opts.autoReconnect),
	)

	if err != nil {
		return nil, err
	}

	sender, err := client.Stream(ctx, func(err error) {
		if err != nil {
			opts.onStreamError(err)
		}
	})

	if err != nil {
		return nil, err
	}

	b := &broker{
		ctx:     ctx,
		client:  client,
		options: opts,
		sender:  sender,
		subs:    make(map[pubsub.Topic]map[string]*subscription),
	}

	return b, nil
}

func (b *broker) Publish(_ context.Context, topic pubsub.Topic, m interface{}) error {
	body, isBinary := m.([]byte)
	if !isBinary {
		return fmt.Errorf("expecting message to be of type []byte, but got `%T`", m)
	}

	sum := sha256.Sum256(body)
	mid := fmt.Sprintf("%x", sum)

	event := kubemq.NewEvent().
		SetId(mid).
		SetChannel(topic.String()).
		SetBody(body)

	return b.sender(event)
}

func (b *broker) Subscribe(_ context.Context, topic pubsub.Topic, subscriber pubsub.Subscriber) error {
	req := &kubemq.EventsSubscription{
		Channel:  topic.String(),
		ClientId: b.options.clientID,
		Group:    b.options.groupID,
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ctx, cancel := context.WithCancel(b.ctx)
	if _, ok := b.subs[topic]; !ok {
		b.subs[topic] = make(map[string]*subscription)
	}

	b.subs[topic][subscriber.ID()] = &subscription{
		cancel:     cancel,
		topic:      topic,
		subscriber: subscriber,
	}

	err := b.client.Subscribe(ctx, req, func(msg *kubemq.Event, err error) {
		if err != nil {
			b.options.onSubscribeError(err)
			return
		}

		if hErr := b.handleRcv(msg, topic, subscriber); hErr != nil {
			// TODO: monitor error
			return
		}
	})

	return err
}

func (b *broker) Unsubscribe(_ context.Context, topic pubsub.Topic, subscriber pubsub.Subscriber) error {
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

	sub.cancel()
	delete(b.subs[topic], subscriber.ID())

	return nil
}

func (b *broker) Subscriptions(_ context.Context) (map[pubsub.Topic][]pubsub.Subscriber, error) {
	out := make(map[pubsub.Topic][]pubsub.Subscriber)

	b.mu.RLock()
	defer b.mu.RUnlock()

	for topic, subs := range b.subs {
		for _, sub := range subs {
			out[topic] = append(out[topic], sub.subscriber)
		}
	}

	return out, nil
}

func (b *broker) Topics(_ context.Context) ([]pubsub.Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]pubsub.Topic, 0, len(b.subs))
	for topic := range b.subs {
		out = append(out, topic)
	}

	return out, nil
}

func (b *broker) Shutdown(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for topic, subs := range b.subs {
		for id, sub := range subs {
			sub.cancel()
			delete(b.subs[topic], id)
		}
	}

	return b.client.Close()
}

func (b *broker) handleRcv(msg *kubemq.Event, topic pubsub.Topic, sub pubsub.Subscriber) error {
	ctx, cancel := context.WithTimeout(b.ctx, b.options.deliverTimeout)
	defer cancel()

	return sub.Deliver(ctx, topic, msg.Body)
}
