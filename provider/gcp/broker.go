package gcp

import (
	"context"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	raw "github.com/botchris/go-pubsub"
	"gocloud.dev/pubsub/gcppubsub"
)

type googlePubSubBroker struct {
	publisher  *pubsub.PublisherClient
	subscriber *pubsub.SubscriberClient
}

func NewBroker(ctx context.Context, projectID string) (raw.Broker, error) {
	publisher, err := gcppubsub.PublisherClient(ctx, nil)
	if err != nil {
		return nil, err
	}

	subscriber, err := gcppubsub.SubscriberClient(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &googlePubSubBroker{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func (g *googlePubSubBroker) Publish(ctx context.Context, topic raw.Topic, m interface{}) error {
	req := &pubsubpb.PublishRequest{
		Topic:    topic.String(),
		Messages: nil,
	}
	_, err := g.publisher.Publish(ctx)

	return err
}

func (g *googlePubSubBroker) Subscribe(ctx context.Context, topic raw.Topic, handler raw.Handler, option ...raw.SubscribeOption) (raw.Subscription, error) {
	//TODO implement me
	panic("implement me")
}

func (g *googlePubSubBroker) Shutdown(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}
