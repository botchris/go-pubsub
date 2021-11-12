package kmq

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"

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
	subscriber *pubsub.Subscriber
}

func NewBroker(ctx context.Context, option ...Option) (pubsub.Broker, error) {
	opts := &options{}
	for _, o := range option {
		o.apply(opts)
	}

	client, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress(opts.serverHost, opts.serverPort),
		kubemq.WithClientId(opts.clientID),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithCheckConnection(true),
		kubemq.WithAutoReconnect(true),
	)

	if err != nil {
		return nil, err
	}

	sender, err := client.Stream(ctx, func(err error) {
		if err != nil {
			fmt.Println(err.Error())
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
	body, err := b.options.encoder(m)
	if err != nil {
		return err
	}

	sum := sha256.Sum256(body)
	mid := fmt.Sprintf("%x", sum)

	event := kubemq.NewEvent().
		SetId(mid).
		SetChannel(topic.String()).
		SetBody(body)

	return b.sender(event)
}

func (b *broker) Subscribe(_ context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
	req := &kubemq.EventsSubscription{
		Channel:  topic.String(),
		Group:    subscriber.ID(),
		ClientId: b.options.clientID,
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

	// TODO: optimize,
	// 	use one single physical subscription per topic, and logically split across local subscribers
	//  (pubsub.Subscriber)
	err := b.client.Subscribe(ctx, req, func(msg *kubemq.Event, err error) {
		if err != nil {
			// TODO: monitor error
			return
		}

		if hErr := b.handleRcv(msg, subscriber); hErr != nil {
			// TODO: monitor error
			return
		}
	})

	return err
}

func (b *broker) Unsubscribe(_ context.Context, topic pubsub.Topic, subscriber *pubsub.Subscriber) error {
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
	return b.client.Close()
}

func (b *broker) handleRcv(msg *kubemq.Event, sub *pubsub.Subscriber) error {
	message, err := b.options.decoder(msg.Body)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(b.ctx, b.options.deliverTimeout)
	defer cancel()

	if dErr := sub.Deliver(ctx, message); dErr != nil {
		return dErr
	}

	return nil
}
