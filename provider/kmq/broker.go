package kmq

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/kmq/rr"
	"github.com/kubemq-io/kubemq-go"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
)

type broker struct {
	ctx     context.Context
	client  *kubemq.EventsClient
	options *options
	sender  func(msg *kubemq.Event) error

	// guards subscribe/unsubscribe operations:
	smu     sync.RWMutex
	subs    *rr.SubscriptionsCollection
	streams *streams
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

	if opts.clientID == "" {
		return nil, errors.New("no client id was provided")
	}

	if opts.groupID == "" {
		return nil, errors.New("no group id was provided")
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
		subs:    rr.NewSubscriptionsCollection(),
		streams: newStreams(),
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
		SetClientId(b.options.clientID).
		SetChannel(topic.String()).
		SetBody(body)

	return b.sender(event)
}

func (b *broker) Subscribe(_ context.Context, topic pubsub.Topic, handler pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	b.smu.Lock()
	defer b.smu.Unlock()

	opts := pubsub.DefaultSubscribeOptions()
	for _, o := range option {
		o(opts)
	}

	sid := uuid.New()
	unsub := func() error {
		b.smu.Lock()
		defer b.smu.Unlock()

		b.subs.RemoveFromTopic(topic, sid)

		// no one listening on this topic anymore.
		if !b.subs.HasTopic(topic) {
			b.streams.remove(topic)
		}

		return nil
	}

	sub, err := pubsub.NewStoppableSubscription(b.ctx, sid, topic, handler, unsub, *opts)
	if err != nil {
		return nil, err
	}

	if !b.streams.has(topic) {
		req := &kubemq.EventsSubscription{
			Channel:  topic.String(),
			ClientId: b.options.clientID,
			Group:    b.options.groupID,
		}

		ctx := b.streams.add(b.ctx, topic)
		sErr := b.client.Subscribe(ctx, req, func(msg *kubemq.Event, err error) {
			if err != nil {
				b.options.onSubscribeError(err)

				return
			}

			if hErr := b.handleRcv(msg, topic); hErr != nil {
				b.options.onSubscribeError(hErr)
			}
		})

		if sErr != nil {
			b.streams.remove(topic)

			return nil, err
		}
	}

	b.subs.Add(sub)

	return sub, nil
}

func (b *broker) Shutdown(_ context.Context) error {
	b.subs.GracefulStop()

	return b.client.Close()
}

func (b *broker) handleRcv(msg *kubemq.Event, topic pubsub.Topic) error {
	subscribers := b.subs.Receptors(topic)
	if len(subscribers) == 0 {
		return nil
	}

	for _, s := range subscribers {
		ctx, cancel := context.WithTimeout(s.Context(), b.options.deliverTimeout)
		err := s.Handler().Deliver(ctx, topic, msg.Body)
		cancel()

		if err != nil {
			b.options.onSubscribeError(err)
		}
	}

	return nil
}
